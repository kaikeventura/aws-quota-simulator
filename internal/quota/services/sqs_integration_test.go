package services

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	fifoQueueTPS     = 5
	quotaSimulatorPort = 4567
	flociPort          = 4566
)

func findProjectDir() string {
	cwd, _ := os.Getwd()
	dir := cwd
	for {
		if _, err := os.Stat(filepath.Join(dir, "Dockerfile")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

func buildTestImage(t *testing.T, projectDir string) string {
	imageName := "quota-simulator-test:latest"
	cmd := exec.Command("docker", "build", "--no-cache", "-t", imageName, ".")
	cmd.Dir = projectDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("Docker build output: %s", string(output))
		t.Fatalf("Failed to build docker image: %v", err)
	}
	return imageName
}

func setupTestContainers(t *testing.T) (testcontainers.Container, int, func()) {
	ctx := context.Background()
	projectDir := findProjectDir()
	require.NotEmpty(t, projectDir, "Could not find project directory")

	imageName := buildTestImage(t, projectDir)

	flociReq := testcontainers.ContainerRequest{
		Image:        "floci/floci:latest",
		ExposedPorts: []string{fmt.Sprintf("%d/tcp", flociPort)},
		Env:          map[string]string{"FLOCI_HOSTNAME": "0.0.0.0"},
		WaitingFor:   wait.ForHTTP("/_localstack/health").WithStartupTimeout(60 * time.Second),
	}

	flociC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: flociReq,
		Started:          true,
	})
	require.NoError(t, err, "Failed to start Floci container")

	flociIP, err := flociC.ContainerIP(ctx)
	require.NoError(t, err, "Failed to get Floci IP")

	quotaSimReq := testcontainers.ContainerRequest{
		Image:        imageName,
		ExposedPorts: []string{fmt.Sprintf("%d/tcp", quotaSimulatorPort)},
		Env: map[string]string{
			"UPSTREAM_URL":      fmt.Sprintf("http://%s:%d", flociIP, flociPort),
			"QUOTA_SQS_FIFO_TPS": strconv.Itoa(fifoQueueTPS),
			"STARTUP_DELAY":     "3",
		},
		WaitingFor: wait.ForHTTP("/health").WithStartupTimeout(60 * time.Second),
	}

	quotaSimC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: quotaSimReq,
		Started:          true,
	})
	require.NoError(t, err, "Failed to start quota-simulator")

	port, err := quotaSimC.MappedPort(ctx, fmt.Sprintf("%d/tcp", quotaSimulatorPort))
	require.NoError(t, err)
	portNum, _ := strconv.Atoi(port.Port())

	cleanup := func() {
		flociC.Terminate(ctx)
		quotaSimC.Terminate(ctx)
	}

	return quotaSimC, portNum, cleanup
}

func createSQSClient(port int) *sqs.Client {
	cfg, _ := config.LoadDefaultConfig(context.Background(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		config.WithRegion("us-east-1"),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: fmt.Sprintf("http://localhost:%d", port)}, nil
			},
		)),
	)
	return sqs.NewFromConfig(cfg)
}

func isThrottleError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Throttl")
}

func TestStandardNoThrottle(t *testing.T) {
	if os.Getenv("SKIP_INTEGRATION") == "1" {
		t.Skip("Skipping integration test")
	}

	_, port, cleanup := setupTestContainers(t)
	defer cleanup()

	client := createSQSClient(port)

	resp, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{
		QueueName: aws.String(fmt.Sprintf("test-%d", time.Now().UnixNano())),
	})
	require.NoError(t, err)

	queueURL := *resp.QueueUrl

	for i := 0; i < 20; i++ {
		_, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{
			QueueUrl:    aws.String(queueURL),
			MessageBody: aws.String("test"),
		})
		require.NoError(t, err, "Standard queue should not throttle")
		time.Sleep(50 * time.Millisecond)
	}

	t.Log("Standard queue: OK - no throttling as expected")
}

func TestFIFOThrottling(t *testing.T) {
	if os.Getenv("SKIP_INTEGRATION") == "1" {
		t.Skip("Skipping integration test")
	}

	_, port, cleanup := setupTestContainers(t)
	defer cleanup()

	client := createSQSClient(port)

	queueName := fmt.Sprintf("test-%d.fifo", time.Now().UnixNano())
	resp, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
		Attributes: map[string]string{
			"FifoQueue":                "true",
			"ContentBasedDeduplication": "true",
		},
	})
	require.NoError(t, err, "Failed to create FIFO queue")

	fifoName := strings.Split(*resp.QueueUrl, "/")[len(strings.Split(*resp.QueueUrl, "/"))-1]
	queueURL := fmt.Sprintf("http://localhost:%d/000000000000/%s", port, fifoName)

	t.Logf("Queue URL: %s", queueURL)
	t.Logf("FIFO TPS configured: %d", fifoQueueTPS)

	msgInput := &sqs.SendMessageInput{
		QueueUrl:       aws.String(queueURL),
		MessageBody:    aws.String("test"),
		MessageGroupId: aws.String("group1"),
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 200)

	numWorkers := 10
	numMessages := 200

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numMessages/numWorkers; j++ {
				_, err := client.SendMessage(context.Background(), msgInput)
				if err != nil {
					errCh <- err
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	throttled := false
	var lastErr error
	
	select {
	case err := <-errCh:
		lastErr = err
		if isThrottleError(err) {
			throttled = true
			t.Logf("SUCCESS: Throttled! Error: %v", err)
		}
	case <-time.After(30 * time.Second):
	}

	wg.Wait()

	if !throttled {
		t.Logf("Sent %d messages without throttle. Last error: %v", numMessages, lastErr)
	}

	require.True(t, throttled, "Expected throttling with TPS=%d, sent %d messages with %d workers", fifoQueueTPS, numMessages, numWorkers)
}