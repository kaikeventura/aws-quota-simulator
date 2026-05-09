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
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	fifoQueueTPS       = 5
	receiveQueueTPS   = 500
	batchQueueTPS      = 5
	standardSendTPS    = 10
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
			"UPSTREAM_URL":                fmt.Sprintf("http://%s:%d", flociIP, flociPort),
			"QUOTA_SQS_FIFO_TPS":          strconv.Itoa(fifoQueueTPS),
			"QUOTA_SQS_FIFO_RECEIVE_TPS":  strconv.Itoa(receiveQueueTPS),
			"QUOTA_SQS_FIFO_DELETE_TPS":   strconv.Itoa(fifoQueueTPS),
			"QUOTA_SQS_FIFO_BATCH_TPS":    strconv.Itoa(batchQueueTPS),
			"QUOTA_SQS_STANDARD_SEND_TPS": strconv.Itoa(standardSendTPS),
			"STARTUP_DELAY":               "3",
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

func TestStandardThrottling(t *testing.T) {
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

	t.Logf("Standard queue URL: %s", queueURL)
	t.Logf("Standard Send TPS configured: %d", standardSendTPS)

	msgInput := &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String("test"),
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
			t.Logf("SUCCESS: Standard queue throttled! Error: %v", err)
		}
	case <-time.After(30 * time.Second):
	}

	wg.Wait()

	if !throttled {
		t.Logf("Sent %d messages without throttle. Last error: %v", numMessages, lastErr)
	}

	require.True(t, throttled, "Expected throttling with Standard TPS=%d, sent %d messages with %d workers", standardSendTPS, numMessages, numWorkers)
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

func TestFIFOReceiveThrottling(t *testing.T) {
	if os.Getenv("SKIP_INTEGRATION") == "1" {
		t.Skip("Skipping integration test")
	}

	_, port, cleanup := setupTestContainers(t)
	defer cleanup()

	client := createSQSClient(port)

	queueName := fmt.Sprintf("test-receive-%d.fifo", time.Now().UnixNano())
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
	t.Logf("Receive TPS configured: %d", receiveQueueTPS)

	sendInput := &sqs.SendMessageInput{
		QueueUrl:       aws.String(queueURL),
		MessageBody:    aws.String("test"),
		MessageGroupId: aws.String("group1"),
	}

	for i := 0; i < 20; i++ {
		client.SendMessage(context.Background(), sendInput)
		time.Sleep(50 * time.Millisecond)
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
				resp, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
					QueueUrl: aws.String(queueURL),
				})
				if err != nil {
					errCh <- err
				}
				_ = resp
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
			t.Logf("SUCCESS: Receive throttled! Error: %v", err)
		}
	case <-time.After(30 * time.Second):
	}

	wg.Wait()

	if !throttled {
		t.Logf("Received %d messages without throttle. Last error: %v", numMessages, lastErr)
	}

	require.True(t, throttled, "Expected receive throttling with TPS=%d", receiveQueueTPS)
}

func TestFIFODeleteThrottling(t *testing.T) {
	if os.Getenv("SKIP_INTEGRATION") == "1" {
		t.Skip("Skipping integration test")
	}

	_, port, cleanup := setupTestContainers(t)
	defer cleanup()

	client := createSQSClient(port)

	queueName := fmt.Sprintf("test-delete-%d.fifo", time.Now().UnixNano())
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
	t.Logf("Batch TPS configured: %d", batchQueueTPS)

	for i := 0; i < 10; i++ {
		groupID := fmt.Sprintf("group-%d", i%5)
		entries := make([]types.SendMessageBatchRequestEntry, 10)
		for j := 0; j < 10; j++ {
			msgID := fmt.Sprintf("msg-%d-%d", i, j)
			entries[j] = types.SendMessageBatchRequestEntry{
				Id:                    aws.String(msgID),
				MessageBody:           aws.String(fmt.Sprintf("test-msg-%d-%d", i, j)),
				MessageGroupId:        aws.String(groupID),
				MessageDeduplicationId: aws.String(fmt.Sprintf("dedup-%d-%d", i, j)),
			}
		}
		client.SendMessageBatch(context.Background(), &sqs.SendMessageBatchInput{
			QueueUrl: aws.String(queueURL),
			Entries:  entries,
		})
		time.Sleep(200 * time.Millisecond)
	}

	time.Sleep(2 * time.Second)

	var receiptHandles []string
	for offset := 0; offset < 3; offset++ {
		msgResp, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     1,
		})
		if err != nil || len(msgResp.Messages) == 0 {
			break
		}
		for _, msg := range msgResp.Messages {
			receiptHandles = append(receiptHandles, *msg.ReceiptHandle)
		}
	}

	t.Logf("Got %d receipt handles", len(receiptHandles))

	if len(receiptHandles) < 5 {
		t.Skipf("Only got %d receipt handles, skipping test", len(receiptHandles))
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 200)

	numWorkers := 10
	numBatches := 50

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(handles []string) {
			defer wg.Done()
			for batchNum := 0; batchNum < numBatches/numWorkers; batchNum++ {
				entries := make([]types.DeleteMessageBatchRequestEntry, 10)
				for j := 0; j < 10; j++ {
					handle := handles[(batchNum*10+j)%len(handles)]
					entries[j] = types.DeleteMessageBatchRequestEntry{
						Id:            aws.String(fmt.Sprintf("del-%d-%d", batchNum, j)),
						ReceiptHandle: aws.String(handle),
					}
				}

				_, err := client.DeleteMessageBatch(context.Background(), &sqs.DeleteMessageBatchInput{
					QueueUrl: aws.String(queueURL),
					Entries:  entries,
				})
				if err != nil {
					errCh <- err
				}
				time.Sleep(20 * time.Millisecond)
			}
		}(receiptHandles)
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
			t.Logf("SUCCESS: DeleteMessageBatch throttled! Error: %v", err)
		}
	case <-time.After(20 * time.Second):
	}

	wg.Wait()

	if !throttled {
		t.Logf("Sent %d batch delete requests without throttle. Last error: %v", numBatches, lastErr)
	}

	require.True(t, throttled, "Expected delete batch throttling with TPS=%d", batchQueueTPS)
}

func isDuplicateError(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "DuplicateMessage") || strings.Contains(err.Error(), "duplicate"))
}

func TestFIFODedupWithMessageDeduplicationId(t *testing.T) {
	if os.Getenv("SKIP_INTEGRATION") == "1" {
		t.Skip("Skipping integration test")
	}

	_, port, cleanup := setupTestContainers(t)
	defer cleanup()

	client := createSQSClient(port)

	queueName := fmt.Sprintf("dedup-test-%d.fifo", time.Now().UnixNano())
	resp, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
		Attributes: map[string]string{
			"FifoQueue":                "true",
			"ContentBasedDeduplication": "false",
		},
	})
	require.NoError(t, err, "Failed to create FIFO queue")

	fifoName := strings.Split(*resp.QueueUrl, "/")[len(strings.Split(*resp.QueueUrl, "/"))-1]
	queueURL := fmt.Sprintf("http://localhost:%d/000000000000/%s", port, fifoName)

	t.Logf("Queue URL: %s", queueURL)

	dedupID := "my-unique-dedup-id-12345"

	_, err = client.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl:               aws.String(queueURL),
		MessageBody:            aws.String("First message"),
		MessageGroupId:         aws.String("group1"),
		MessageDeduplicationId: aws.String(dedupID),
	})
	require.NoError(t, err, "First send should succeed")

	_, err = client.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl:               aws.String(queueURL),
		MessageBody:            aws.String("Duplicate message"),
		MessageGroupId:         aws.String("group1"),
		MessageDeduplicationId: aws.String(dedupID),
	})

	require.Error(t, err, "Second send with same dedup ID should fail")
	require.True(t, isDuplicateError(err), "Should return duplicate message error, got: %v", err)

	t.Log("SUCCESS: DuplicateMessage error returned for duplicate dedup ID")
}

func TestFIFODedupWithContentBasedDeduplication(t *testing.T) {
	if os.Getenv("SKIP_INTEGRATION") == "1" {
		t.Skip("Skipping integration test")
	}

	_, port, cleanup := setupTestContainers(t)
	defer cleanup()

	client := createSQSClient(port)

	queueName := fmt.Sprintf("content-dedup-%d.fifo", time.Now().UnixNano())
	resp, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
		Attributes: map[string]string{
			"FifoQueue":                "true",
			"ContentBasedDeduplication": "true",
		},
	})
	require.NoError(t, err, "Failed to create FIFO queue with ContentBasedDeduplication")

	fifoName := strings.Split(*resp.QueueUrl, "/")[len(strings.Split(*resp.QueueUrl, "/"))-1]
	queueURL := fmt.Sprintf("http://localhost:%d/000000000000/%s", port, fifoName)

	t.Logf("Queue URL: %s", queueURL)

	_, err = client.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl:       aws.String(queueURL),
		MessageBody:    aws.String("Same body content"),
		MessageGroupId: aws.String("group1"),
	})
	require.NoError(t, err, "First send should succeed")

	_, err = client.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl:       aws.String(queueURL),
		MessageBody:    aws.String("Same body content"),
		MessageGroupId: aws.String("group1"),
	})

	require.Error(t, err, "Second send with same body should fail with content-based dedup")
	require.True(t, isDuplicateError(err), "Should return duplicate message error for same body, got: %v", err)

	t.Log("SUCCESS: DuplicateMessage error returned for content-based deduplication")
}

func TestFIFONoDedupWithoutContentBasedDeduplication(t *testing.T) {
	if os.Getenv("SKIP_INTEGRATION") == "1" {
		t.Skip("Skipping integration test")
	}

	_, port, cleanup := setupTestContainers(t)
	defer cleanup()

	client := createSQSClient(port)

	queueName := fmt.Sprintf("no-dedup-%d.fifo", time.Now().UnixNano())
	resp, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
		Attributes: map[string]string{
			"FifoQueue":                "true",
			"ContentBasedDeduplication": "false",
		},
	})
	require.NoError(t, err, "Failed to create FIFO queue")

	fifoName := strings.Split(*resp.QueueUrl, "/")[len(strings.Split(*resp.QueueUrl, "/"))-1]
	queueURL := fmt.Sprintf("http://localhost:%d/000000000000/%s", port, fifoName)

	t.Logf("Queue URL: %s", queueURL)

	_, err = client.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl:       aws.String(queueURL),
		MessageBody:    aws.String("Same body without dedup ID"),
		MessageGroupId: aws.String("group1"),
	})
	require.NoError(t, err, "First send should succeed")

	_, err = client.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl:       aws.String(queueURL),
		MessageBody:    aws.String("Same body without dedup ID"),
		MessageGroupId: aws.String("group1"),
	})

	require.NoError(t, err, "Without MessageDeduplicationId and ContentBasedDeduplication=false, same body should be allowed")
	t.Log("SUCCESS: Same body allowed when no deduplication is configured")
}

func TestFIFODedupDifferentGroups(t *testing.T) {
	if os.Getenv("SKIP_INTEGRATION") == "1" {
		t.Skip("Skipping integration test")
	}

	_, port, cleanup := setupTestContainers(t)
	defer cleanup()

	client := createSQSClient(port)

	queueName := fmt.Sprintf("groups-dedup-%d.fifo", time.Now().UnixNano())
	resp, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
		Attributes: map[string]string{
			"FifoQueue":                "true",
			"ContentBasedDeduplication": "false",
		},
	})
	require.NoError(t, err, "Failed to create FIFO queue")

	fifoName := strings.Split(*resp.QueueUrl, "/")[len(strings.Split(*resp.QueueUrl, "/"))-1]
	queueURL := fmt.Sprintf("http://localhost:%d/000000000000/%s", port, fifoName)

	t.Logf("Queue URL: %s", queueURL)

	dedupID := "shared-dedup-id"

	_, err = client.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl:               aws.String(queueURL),
		MessageBody:            aws.String("Message in group 1"),
		MessageGroupId:         aws.String("group1"),
		MessageDeduplicationId: aws.String(dedupID),
	})
	require.NoError(t, err, "First send should succeed")

	_, err = client.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl:               aws.String(queueURL),
		MessageBody:            aws.String("Message in group 2"),
		MessageGroupId:         aws.String("group2"),
		MessageDeduplicationId: aws.String(dedupID),
	})

	require.NoError(t, err, "Same dedup ID in different groups should be allowed")
	t.Log("SUCCESS: Same dedup ID allowed in different message groups")
}