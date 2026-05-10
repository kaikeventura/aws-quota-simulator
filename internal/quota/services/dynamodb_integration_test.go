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
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	onDemandReadTPS   = 50
	onDemandWriteTPS  = 50
	provisionedRCU   = 10
	provisionedWCU   = 10
	dynamoPort       = 4567
	dynamoUpstreamPort = 4566
)

func findProjectDirDynamo() string {
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

func buildTestImageDynamo(t *testing.T, projectDir string) string {
	imageName := "quota-simulator-test-dynamo:latest"
	cmd := exec.Command("docker", "build", "--no-cache", "-t", imageName, ".")
	cmd.Dir = projectDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("Docker build output: %s", string(output))
		t.Fatalf("Failed to build docker image: %v", err)
	}
	return imageName
}

func setupTestContainersDynamo(t *testing.T) (testcontainers.Container, int, func()) {
	ctx := context.Background()
	projectDir := findProjectDirDynamo()
	require.NotEmpty(t, projectDir, "Could not find project directory")

	imageName := buildTestImageDynamo(t, projectDir)

	flociReq := testcontainers.ContainerRequest{
		Image:        "floci/floci:latest",
		ExposedPorts: []string{fmt.Sprintf("%d/tcp", dynamoUpstreamPort)},
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
		ExposedPorts: []string{fmt.Sprintf("%d/tcp", dynamoPort)},
		Env: map[string]string{
			"UPSTREAM_URL":                      fmt.Sprintf("http://%s:%d", flociIP, dynamoUpstreamPort),
			"QUOTA_DYNAMODB_ONDEMAND_MAX_READ_RPS":  strconv.Itoa(onDemandReadTPS),
			"QUOTA_DYNAMODB_ONDEMAND_MAX_WRITE_RPS": strconv.Itoa(onDemandWriteTPS),
			"QUOTA_DYNAMODB_ONDEMAND_INITIAL_READ_RPS":  "20",
			"QUOTA_DYNAMODB_ONDEMAND_INITIAL_WRITE_RPS": "20",
			"QUOTA_DYNAMODB_PROVISIONED_PER_TABLE_RCU": strconv.Itoa(provisionedRCU),
			"QUOTA_DYNAMODB_PROVISIONED_PER_TABLE_WCU": strconv.Itoa(provisionedWCU),
			"STARTUP_DELAY":                     "3",
		},
		WaitingFor: wait.ForHTTP("/health").WithStartupTimeout(60 * time.Second),
	}

	quotaSimC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: quotaSimReq,
		Started:          true,
	})
	require.NoError(t, err, "Failed to start quota-simulator")

	port, err := quotaSimC.MappedPort(ctx, fmt.Sprintf("%d/tcp", dynamoPort))
	require.NoError(t, err)
	portNum, _ := strconv.Atoi(port.Port())

	cleanup := func() {
		flociC.Terminate(ctx)
		quotaSimC.Terminate(ctx)
	}

	return quotaSimC, portNum, cleanup
}

func createDynamoDBClient(port int) *dynamodb.Client {
	cfg, _ := config.LoadDefaultConfig(context.Background(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		config.WithRegion("us-east-1"),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: fmt.Sprintf("http://localhost:%d", port)}, nil
			},
		)),
	)
	return dynamodb.NewFromConfig(cfg)
}

func isThrottleErrorDynamo(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "Throttl") ||
		strings.Contains(errStr, "ProvisionedThroughputExceededException") ||
		strings.Contains(errStr, "RequestLimitExceeded")
}

func TestOnDemandTableThrottling(t *testing.T) {
	if os.Getenv("SKIP_INTEGRATION") == "1" {
		t.Skip("Skipping integration test")
	}

	_, port, cleanup := setupTestContainersDynamo(t)
	defer cleanup()

	client := createDynamoDBClient(port)

	tableName := fmt.Sprintf("test-ondemand-%d", time.Now().UnixNano())

	_, err := client.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err, "Failed to create on-demand table")

	time.Sleep(2 * time.Second)

	t.Logf("Created on-demand table: %s", tableName)
	t.Logf("On-Demand Read TPS configured: %d", onDemandReadTPS)
	t.Logf("On-Demand Write TPS configured: %d", onDemandWriteTPS)

	var wg sync.WaitGroup
	errCh := make(chan error, 500)

	numWorkers := 10
	numOperations := 500

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations/numWorkers; j++ {
				_, err := client.PutItem(context.Background(), &dynamodb.PutItemInput{
					TableName: aws.String(tableName),
					Item: map[string]types.AttributeValue{
						"id":   &types.AttributeValueMemberS{Value: fmt.Sprintf("item-%d-%d", i, j)},
						"data": &types.AttributeValueMemberS{Value: strings.Repeat("x", 100)},
					},
				})
				if err != nil {
					errCh <- err
				}
				time.Sleep(5 * time.Millisecond)
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
		if isThrottleErrorDynamo(err) {
			throttled = true
			t.Logf("SUCCESS: On-demand table throttled! Error: %v", err)
		}
	case <-time.After(60 * time.Second):
	}

	wg.Wait()

	if !throttled {
		t.Logf("Sent %d operations without throttle. Last error: %v", numOperations, lastErr)
	}

	require.True(t, throttled, "Expected throttling with On-Demand Write TPS=%d, sent %d operations", onDemandWriteTPS, numOperations)
}

func TestProvisionedTableThrottling(t *testing.T) {
	if os.Getenv("SKIP_INTEGRATION") == "1" {
		t.Skip("Skipping integration test")
	}

	_, port, cleanup := setupTestContainersDynamo(t)
	defer cleanup()

	client := createDynamoDBClient(port)

	tableName := fmt.Sprintf("test-provisioned-%d", time.Now().UnixNano())

	_, err := client.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(provisionedRCU),
			WriteCapacityUnits: aws.Int64(provisionedWCU),
		},
	})
	require.NoError(t, err, "Failed to create provisioned table")

	time.Sleep(2 * time.Second)

	t.Logf("Created provisioned table: %s", tableName)
	t.Logf("Provisioned RCU: %d, WCU: %d", provisionedRCU, provisionedWCU)

	var wg sync.WaitGroup
	errCh := make(chan error, 500)

	numWorkers := 10
	numOperations := 300

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations/numWorkers; j++ {
				_, err := client.PutItem(context.Background(), &dynamodb.PutItemInput{
					TableName: aws.String(tableName),
					Item: map[string]types.AttributeValue{
						"id":   &types.AttributeValueMemberS{Value: fmt.Sprintf("item-%d-%d", i, j)},
						"data": &types.AttributeValueMemberS{Value: strings.Repeat("x", 100)},
					},
				})
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
		if isThrottleErrorDynamo(err) {
			throttled = true
			t.Logf("SUCCESS: Provisioned table throttled! Error: %v", err)
		}
	case <-time.After(60 * time.Second):
	}

	wg.Wait()

	if !throttled {
		t.Logf("Sent %d operations without throttle. Last error: %v", numOperations, lastErr)
	}

	require.True(t, throttled, "Expected throttling with Provisioned WCU=%d, sent %d operations", provisionedWCU, numOperations)
}

func TestProvisionedReadThrottling(t *testing.T) {
	if os.Getenv("SKIP_INTEGRATION") == "1" {
		t.Skip("Skipping integration test")
	}

	_, port, cleanup := setupTestContainersDynamo(t)
	defer cleanup()

	client := createDynamoDBClient(port)

	tableName := fmt.Sprintf("test-read-provisioned-%d", time.Now().UnixNano())

	_, err := client.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(provisionedRCU),
			WriteCapacityUnits: aws.Int64(provisionedWCU),
		},
	})
	require.NoError(t, err, "Failed to create provisioned table")

	time.Sleep(2 * time.Second)

	for i := 0; i < 50; i++ {
		client.PutItem(context.Background(), &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"id":   &types.AttributeValueMemberS{Value: fmt.Sprintf("item-%d", i)},
				"data": &types.AttributeValueMemberS{Value: "testdata"},
			},
		})
	}
	time.Sleep(1 * time.Second)

	t.Logf("Created provisioned table for read test: %s", tableName)
	t.Logf("Provisioned RCU: %d", provisionedRCU)

	var wg sync.WaitGroup
	errCh := make(chan error, 500)

	numWorkers := 10
	numOperations := 300

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations/numWorkers; j++ {
				_, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
					TableName: aws.String(tableName),
					Key: map[string]types.AttributeValue{
						"id": &types.AttributeValueMemberS{Value: fmt.Sprintf("item-%d", j%50)},
					},
				})
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
		if isThrottleErrorDynamo(err) {
			throttled = true
			t.Logf("SUCCESS: Provisioned read throttled! Error: %v", err)
		}
	case <-time.After(60 * time.Second):
	}

	wg.Wait()

	if !throttled {
		t.Logf("Sent %d read operations without throttle. Last error: %v", numOperations, lastErr)
	}

	require.True(t, throttled, "Expected read throttling with Provisioned RCU=%d, sent %d operations", provisionedRCU, numOperations)
}

func TestOnDemandQueryThrottling(t *testing.T) {
	if os.Getenv("SKIP_INTEGRATION") == "1" {
		t.Skip("Skipping integration test")
	}

	_, port, cleanup := setupTestContainersDynamo(t)
	defer cleanup()

	client := createDynamoDBClient(port)

	tableName := fmt.Sprintf("test-query-ondemand-%d", time.Now().UnixNano())

	_, err := client.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err, "Failed to create on-demand table")

	time.Sleep(2 * time.Second)

	t.Logf("Created on-demand table for query test: %s", tableName)
	t.Logf("On-Demand Read TPS configured: %d", onDemandReadTPS)

	var wg sync.WaitGroup
	errCh := make(chan error, 200)

	numWorkers := 10
	numOperations := 200

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations/numWorkers; j++ {
				_, err := client.Scan(context.Background(), &dynamodb.ScanInput{
					TableName: aws.String(tableName),
				})
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
		if isThrottleErrorDynamo(err) {
			throttled = true
			t.Logf("SUCCESS: On-demand scan throttled! Error: %v", err)
		}
	case <-time.After(60 * time.Second):
	}

	wg.Wait()

	if !throttled {
		t.Logf("Sent %d scan operations without throttle. Last error: %v", numOperations, lastErr)
	}

	require.True(t, throttled, "Expected scan throttling with On-Demand Read TPS=%d", onDemandReadTPS)
}

func TestOnDemandBatchWriteThrottling(t *testing.T) {
	if os.Getenv("SKIP_INTEGRATION") == "1" {
		t.Skip("Skipping integration test")
	}

	_, port, cleanup := setupTestContainersDynamo(t)
	defer cleanup()

	client := createDynamoDBClient(port)

	tableName := fmt.Sprintf("test-batch-ondemand-%d", time.Now().UnixNano())

	_, err := client.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err, "Failed to create on-demand table")

	time.Sleep(2 * time.Second)

	t.Logf("Created on-demand table: %s", tableName)

	var wg sync.WaitGroup
	errCh := make(chan error, 100)

	numBatches := 100

	for i := 0; i < numBatches; i++ {
		wg.Add(1)
		go func(batchNum int) {
			defer wg.Done()

			entries := make([]types.WriteRequest, 25)
			for j := 0; j < 25; j++ {
				entries[j] = types.WriteRequest{
					PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"id":   &types.AttributeValueMemberS{Value: fmt.Sprintf("batch-%d-item-%d", batchNum, j)},
							"data": &types.AttributeValueMemberS{Value: "test"},
						},
					},
				}
			}

			_, err := client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{
					tableName: entries,
				},
			})
			if err != nil {
				errCh <- err
			}
			time.Sleep(20 * time.Millisecond)
		}(i)
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
		if isThrottleErrorDynamo(err) {
			throttled = true
			t.Logf("SUCCESS: On-demand BatchWriteItem throttled! Error: %v", err)
		}
	case <-time.After(60 * time.Second):
	}

	wg.Wait()

	if !throttled {
		t.Logf("Sent %d batch writes without throttle. Last error: %v", numBatches, lastErr)
	}

	require.True(t, throttled, "Expected BatchWriteItem throttling with On-Demand TPS=%d", onDemandWriteTPS)
}

func TestProvisionedBatchGetThrottling(t *testing.T) {
	if os.Getenv("SKIP_INTEGRATION") == "1" {
		t.Skip("Skipping integration test")
	}

	_, port, cleanup := setupTestContainersDynamo(t)
	defer cleanup()

	client := createDynamoDBClient(port)

	tableName := fmt.Sprintf("test-batch-get-provisioned-%d", time.Now().UnixNano())

	_, err := client.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(provisionedRCU),
			WriteCapacityUnits: aws.Int64(provisionedWCU),
		},
	})
	require.NoError(t, err, "Failed to create provisioned table")

	time.Sleep(2 * time.Second)

	for i := 0; i < 100; i++ {
		client.PutItem(context.Background(), &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"id":   &types.AttributeValueMemberS{Value: fmt.Sprintf("item-%d", i)},
				"data": &types.AttributeValueMemberS{Value: "testdata"},
			},
		})
	}
	time.Sleep(1 * time.Second)

	t.Logf("Created provisioned table for batch get test: %s", tableName)
	t.Logf("Provisioned RCU: %d", provisionedRCU)

	var wg sync.WaitGroup
	errCh := make(chan error, 100)

	numBatches := 100

	for i := 0; i < numBatches; i++ {
		wg.Add(1)
		go func(batchNum int) {
			defer wg.Done()

			keys := make([]map[string]types.AttributeValue, 100)
			for j := 0; j < 100; j++ {
				keys[j] = map[string]types.AttributeValue{
					"id": &types.AttributeValueMemberS{Value: fmt.Sprintf("item-%d", j%100)},
				}
			}

			_, err := client.BatchGetItem(context.Background(), &dynamodb.BatchGetItemInput{
				RequestItems: map[string]types.KeysAndAttributes{
					tableName: {
						Keys:            keys,
						ConsistentRead:  aws.Bool(false),
					},
				},
			})
			if err != nil {
				errCh <- err
			}
			time.Sleep(20 * time.Millisecond)
		}(i)
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
		if isThrottleErrorDynamo(err) {
			throttled = true
			t.Logf("SUCCESS: Provisioned BatchGetItem throttled! Error: %v", err)
		}
	case <-time.After(60 * time.Second):
	}

	wg.Wait()

	if !throttled {
		t.Logf("Sent %d batch reads without throttle. Last error: %v", numBatches, lastErr)
	}

	require.True(t, throttled, "Expected BatchGetItem throttling with Provisioned RCU=%d", provisionedRCU)
}

func TestProvisionedBatchWriteThrottling(t *testing.T) {
	if os.Getenv("SKIP_INTEGRATION") == "1" {
		t.Skip("Skipping integration test")
	}

	_, port, cleanup := setupTestContainersDynamo(t)
	defer cleanup()

	client := createDynamoDBClient(port)

	tableName := fmt.Sprintf("test-batch-write-provisioned-%d", time.Now().UnixNano())

	_, err := client.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(provisionedRCU),
			WriteCapacityUnits: aws.Int64(provisionedWCU),
		},
	})
	require.NoError(t, err, "Failed to create provisioned table")

	time.Sleep(2 * time.Second)

	t.Logf("Created provisioned table: %s", tableName)
	t.Logf("Provisioned WCU: %d", provisionedWCU)

	var wg sync.WaitGroup
	errCh := make(chan error, 100)

	numBatches := 50

	for i := 0; i < numBatches; i++ {
		wg.Add(1)
		go func(batchNum int) {
			defer wg.Done()

			entries := make([]types.WriteRequest, 25)
			for j := 0; j < 25; j++ {
				entries[j] = types.WriteRequest{
					PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"id":   &types.AttributeValueMemberS{Value: fmt.Sprintf("batch-%d-item-%d", batchNum, j)},
							"data": &types.AttributeValueMemberS{Value: "test"},
						},
					},
				}
			}

			_, err := client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{
					tableName: entries,
				},
			})
			if err != nil {
				errCh <- err
			}
			time.Sleep(30 * time.Millisecond)
		}(i)
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
		if isThrottleErrorDynamo(err) {
			throttled = true
			t.Logf("SUCCESS: Provisioned BatchWriteItem throttled! Error: %v", err)
		}
	case <-time.After(60 * time.Second):
	}

	wg.Wait()

	if !throttled {
		t.Logf("Sent %d batch writes without throttle. Last error: %v", numBatches, lastErr)
	}

	require.True(t, throttled, "Expected BatchWriteItem throttling with Provisioned WCU=%d", provisionedWCU)
}

func TestOnDemandBatchGetThrottling(t *testing.T) {
	if os.Getenv("SKIP_INTEGRATION") == "1" {
		t.Skip("Skipping integration test")
	}

	_, port, cleanup := setupTestContainersDynamo(t)
	defer cleanup()

	client := createDynamoDBClient(port)

	tableName := fmt.Sprintf("test-batch-get-ondemand-%d", time.Now().UnixNano())

	_, err := client.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err, "Failed to create on-demand table")

	time.Sleep(2 * time.Second)

	for i := 0; i < 50; i++ {
		client.PutItem(context.Background(), &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"id":   &types.AttributeValueMemberS{Value: fmt.Sprintf("item-%d", i)},
				"data": &types.AttributeValueMemberS{Value: "testdata"},
			},
		})
	}
	time.Sleep(1 * time.Second)

	t.Logf("Created on-demand table for batch get: %s", tableName)
	t.Logf("On-Demand Read TPS: %d", onDemandReadTPS)

	var wg sync.WaitGroup
	errCh := make(chan error, 100)

	numBatches := 50

	for i := 0; i < numBatches; i++ {
		wg.Add(1)
		go func(batchNum int) {
			defer wg.Done()

			keys := make([]map[string]types.AttributeValue, 50)
			for j := 0; j < 50; j++ {
				keys[j] = map[string]types.AttributeValue{
					"id": &types.AttributeValueMemberS{Value: fmt.Sprintf("item-%d", j%50)},
				}
			}

			_, err := client.BatchGetItem(context.Background(), &dynamodb.BatchGetItemInput{
				RequestItems: map[string]types.KeysAndAttributes{
					tableName: {
						Keys:            keys,
						ConsistentRead:  aws.Bool(false),
					},
				},
			})
			if err != nil {
				errCh <- err
			}
			time.Sleep(30 * time.Millisecond)
		}(i)
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
		if isThrottleErrorDynamo(err) {
			throttled = true
			t.Logf("SUCCESS: On-demand BatchGetItem throttled! Error: %v", err)
		}
	case <-time.After(60 * time.Second):
	}

	wg.Wait()

	if !throttled {
		t.Logf("Sent %d batch reads without throttle. Last error: %v", numBatches, lastErr)
	}

	require.True(t, throttled, "Expected BatchGetItem throttling with On-Demand Read TPS=%d", onDemandReadTPS)
}

func TestProvisionedQueryThrottling(t *testing.T) {
	if os.Getenv("SKIP_INTEGRATION") == "1" {
		t.Skip("Skipping integration test")
	}

	_, port, cleanup := setupTestContainersDynamo(t)
	defer cleanup()

	client := createDynamoDBClient(port)

	tableName := fmt.Sprintf("test-query-provisioned-%d", time.Now().UnixNano())

	_, err := client.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(provisionedRCU),
			WriteCapacityUnits: aws.Int64(provisionedWCU),
		},
	})
	require.NoError(t, err, "Failed to create provisioned table")

	time.Sleep(2 * time.Second)

	for i := 0; i < 100; i++ {
		client.PutItem(context.Background(), &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"id":   &types.AttributeValueMemberS{Value: fmt.Sprintf("item-%d", i)},
				"data": &types.AttributeValueMemberS{Value: "testdata"},
			},
		})
	}
	time.Sleep(1 * time.Second)

	t.Logf("Created provisioned table for query test: %s", tableName)
	t.Logf("Provisioned RCU: %d", provisionedRCU)

	var wg sync.WaitGroup
	errCh := make(chan error, 200)

	numWorkers := 10
	numOperations := 200

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations/numWorkers; j++ {
				_, err := client.Query(context.Background(), &dynamodb.QueryInput{
					TableName:              aws.String(tableName),
					KeyConditionExpression: aws.String("id = :id"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":id": &types.AttributeValueMemberS{Value: "item-1"},
					},
				})
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
		if isThrottleErrorDynamo(err) {
			throttled = true
			t.Logf("SUCCESS: Provisioned Query throttled! Error: %v", err)
		}
	case <-time.After(60 * time.Second):
	}

	wg.Wait()

	if !throttled {
		t.Logf("Sent %d query operations without throttle. Last error: %v", numOperations, lastErr)
	}

	require.True(t, throttled, "Expected Query throttling with Provisioned RCU=%d", provisionedRCU)
}