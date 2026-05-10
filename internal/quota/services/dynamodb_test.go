package services

import (
	"testing"

	"github.com/aws-quota-simulator/internal/config"
	"github.com/stretchr/testify/assert"
)

func TestDynamoDBService_ParseTableName(t *testing.T) {
	cfg := &config.Config{
		DynamoDB: config.DynamoDBConfig{
			OnDemand: config.OnDemandConfig{
				MaxTableReadRPS:  40000,
				MaxTableWriteRPS: 40000,
			},
		},
	}
	svc := NewDynamoDBService(cfg)

	tests := []struct {
		name     string
		body     string
		expected string
	}{
		{
			"PutItem with TableName",
			`{"TableName":"my-table","Item":{"id":{"S":"123"}}}`,
			"my-table",
		},
		{
			"GetItem with TableName",
			`{"TableName":"users","Key":{"id":{"S":"user1"}}}`,
			"users",
		},
		{
			"Empty body",
			``,
			"",
		},
		{
			"Invalid JSON",
			`{invalid}`,
			"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := svc.ParseTableName([]byte(tt.body))
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDynamoDBService_DetectOperation(t *testing.T) {
	cfg := &config.Config{}
	svc := NewDynamoDBService(cfg)

	tests := []struct {
		name     string
		path     string
		expected string
	}{
		{"PutItem via query", "/?Action=PutItem", "put_item"},
		{"GetItem via query", "/?Action=GetItem", "get_item"},
		{"DeleteItem via query", "/?Action=DeleteItem", "delete_item"},
		{"UpdateItem via query", "/?Action=UpdateItem", "update_item"},
		{"Query via query", "/?Action=Query", "query"},
		{"Scan via query", "/?Action=Scan", "scan"},
		{"CreateTable via query", "/?Action=CreateTable", "create_table"},
		{"DescribeLimits via query", "/?Action=DescribeLimits", "describe_limits"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			operation, _ := svc.DetectOperation(tt.path, "")
			assert.Equal(t, tt.expected, operation)
		})
	}
}

func TestDynamoDBService_IsReadOperation(t *testing.T) {
	cfg := &config.Config{}
	svc := NewDynamoDBService(cfg)

	tests := []struct {
		operation string
		expected  bool
	}{
		{"get_item", true},
		{"query", true},
		{"scan", true},
		{"put_item", false},
		{"delete_item", false},
		{"update_item", false},
	}

	for _, tt := range tests {
		t.Run(tt.operation, func(t *testing.T) {
			result := svc.IsReadOperation(tt.operation)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDynamoDBService_IsWriteOperation(t *testing.T) {
	cfg := &config.Config{}
	svc := NewDynamoDBService(cfg)

	tests := []struct {
		operation string
		expected  bool
	}{
		{"put_item", true},
		{"delete_item", true},
		{"update_item", true},
		{"create_table", true},
		{"get_item", false},
		{"query", false},
		{"scan", false},
	}

	for _, tt := range tests {
		t.Run(tt.operation, func(t *testing.T) {
			result := svc.IsWriteOperation(tt.operation)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDynamoDBService_Defaults(t *testing.T) {
	cfg := &config.Config{
		DynamoDB: config.DynamoDBConfig{
			OnDemand: config.OnDemandConfig{
				MaxTableReadRPS:  40000,
				MaxTableWriteRPS: 40000,
			},
			Provisioned: config.ProvisionedConfig{
				AccountMaxRCU: 80000,
				AccountMaxWCU: 80000,
			},
			Default: config.DynamoDBDefaultConfig{
				TableMaxRCU: 10000,
				TableMaxWCU: 10000,
			},
		},
	}
	svc := NewDynamoDBService(cfg)

	assert.Equal(t, int64(40000), svc.GetOnDemandMaxReadRPS())
	assert.Equal(t, int64(40000), svc.GetOnDemandMaxWriteRPS())
	assert.Equal(t, int64(80000), svc.GetAccountMaxRCU())
	assert.Equal(t, int64(80000), svc.GetAccountMaxWCU())
	assert.Equal(t, int64(10000), svc.GetDefaultRCU())
	assert.Equal(t, int64(10000), svc.GetDefaultWCU())
}

func TestDynamoDBService_ParseCreateTableRequest(t *testing.T) {
	cfg := &config.Config{}
	svc := NewDynamoDBService(cfg)

	tests := []struct {
		name         string
		body         string
		expectedName string
		expectedMode BillingMode
		expectedRCU  int64
		expectedWCU  int64
	}{
		{
			"Provisioned with RCU/WCU",
			`{"TableName":"test-table","BillingMode":"PROVISIONED","ProvisionedThroughput":{"ReadCapacityUnits":100,"WriteCapacityUnits":50}}`,
			"test-table",
			BillingModeProvisioned,
			100,
			50,
		},
		{
			"On-Demand mode",
			`{"TableName":"ondemand-table","BillingMode":"PAY_PER_REQUEST"}`,
			"ondemand-table",
			BillingModeOnDemand,
			0,
			0,
		},
		{
			"Default provisioned",
			`{"TableName":"default-table","ProvisionedThroughput":{"ReadCapacityUnits":10}}`,
			"default-table",
			BillingModeProvisioned,
			10,
			0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, mode, rcu, wcu := svc.ParseCreateTableRequest([]byte(tt.body))
			assert.Equal(t, tt.expectedName, name)
			assert.Equal(t, tt.expectedMode, mode)
			assert.Equal(t, tt.expectedRCU, rcu)
			assert.Equal(t, tt.expectedWCU, wcu)
		})
	}
}

func TestDynamoDBService_RegisterAndGetTableMetadata(t *testing.T) {
	cfg := &config.Config{
		DynamoDB: config.DynamoDBConfig{
			OnDemand: config.OnDemandConfig{
				InitialReadRPS:  2000,
				InitialWriteRPS: 2000,
			},
		},
	}
	svc := NewDynamoDBService(cfg)

	svc.RegisterTable("table1", BillingModeProvisioned, 100, 50)
	svc.RegisterTable("table2", BillingModeOnDemand, 0, 0)

	meta1, ok1 := svc.GetTableMetadata("table1")
	assert.True(t, ok1)
	assert.Equal(t, "table1", meta1.TableName)
	assert.Equal(t, BillingModeProvisioned, meta1.BillingMode)
	assert.Equal(t, int64(100), meta1.ProvisionedRCU)
	assert.Equal(t, int64(50), meta1.ProvisionedWCU)

	meta2, ok2 := svc.GetTableMetadata("table2")
	assert.True(t, ok2)
	assert.Equal(t, BillingModeOnDemand, meta2.BillingMode)
	assert.Equal(t, int64(2000), meta2.ProvisionedRCU)

	_, ok3 := svc.GetTableMetadata("nonexistent")
	assert.False(t, ok3)
}

func TestDynamoDBService_GetBillingMode(t *testing.T) {
	cfg := &config.Config{}
	svc := NewDynamoDBService(cfg)

	svc.RegisterTable("provisioned-table", BillingModeProvisioned, 100, 50)
	svc.RegisterTable("ondemand-table", BillingModeOnDemand, 0, 0)

	assert.Equal(t, BillingModeProvisioned, svc.GetBillingMode("provisioned-table"))
	assert.Equal(t, BillingModeOnDemand, svc.GetBillingMode("ondemand-table"))
	assert.Equal(t, BillingModeOnDemand, svc.GetBillingMode("unknown-table"))
}

func TestDynamoDBService_CalculateRCU(t *testing.T) {
	cfg := &config.Config{}
	svc := NewDynamoDBService(cfg)

	assert.Equal(t, int64(1), svc.CalculateRCU("get_item", 4096))
	assert.Equal(t, int64(2), svc.CalculateRCU("get_item", 8192))
	assert.Equal(t, int64(1), svc.CalculateRCU("query", 4096))
	assert.Equal(t, int64(1), svc.CalculateRCU("scan", 4096))
	assert.Equal(t, int64(1), svc.CalculateRCU("unknown", 0))
}

func TestDynamoDBService_CalculateWCU(t *testing.T) {
	cfg := &config.Config{}
	svc := NewDynamoDBService(cfg)

	assert.Equal(t, int64(1), svc.CalculateWCU("put_item", 1024))
	assert.Equal(t, int64(2), svc.CalculateWCU("put_item", 2048))
	assert.Equal(t, int64(1), svc.CalculateWCU("delete_item", 1024))
	assert.Equal(t, int64(1), svc.CalculateWCU("update_item", 1024))
	assert.Equal(t, int64(1), svc.CalculateWCU("unknown", 0))
}

func TestDynamoDBService_EstimateItemSize(t *testing.T) {
	cfg := &config.Config{}
	svc := NewDynamoDBService(cfg)

	size1 := svc.EstimateItemSize([]byte(`{"Item":{"id":{"S":"12345"},"data":{"S":"abcdefghijklmnopqrstuvwxyz"}}}`))
	assert.Greater(t, size1, int64(0))

	size2 := svc.EstimateItemSize([]byte(`{"PutItem":{"id":{"S":"test"}}}`))
	assert.Greater(t, size2, int64(0))

	size3 := svc.EstimateItemSize([]byte(`{}`))
	assert.Equal(t, int64(1024), size3)
}

func TestDynamoDBService_AccountUsageTracking(t *testing.T) {
	cfg := &config.Config{}
	svc := NewDynamoDBService(cfg)

	assert.Equal(t, int64(0), svc.GetAccountUsedRCU())
	assert.Equal(t, int64(0), svc.GetAccountUsedWCU())

	svc.ConsumeAccountRCU(100)
	svc.ConsumeAccountWCU(50)

	assert.Equal(t, int64(100), svc.GetAccountUsedRCU())
	assert.Equal(t, int64(50), svc.GetAccountUsedWCU())

	svc.ReleaseAccountRCU(30)
	svc.ReleaseAccountWCU(20)

	assert.Equal(t, int64(70), svc.GetAccountUsedRCU())
	assert.Equal(t, int64(30), svc.GetAccountUsedWCU())
}

func TestDynamoDBService_InitialRPS(t *testing.T) {
	cfg := &config.Config{
		DynamoDB: config.DynamoDBConfig{
			OnDemand: config.OnDemandConfig{
				InitialReadRPS:  3000,
				InitialWriteRPS: 4000,
			},
		},
	}
	svc := NewDynamoDBService(cfg)

	assert.Equal(t, int64(3000), svc.GetOnDemandInitialReadRPS())
	assert.Equal(t, int64(4000), svc.GetOnDemandInitialWriteRPS())
}

func TestDynamoDBService_ProvisionedLimits(t *testing.T) {
	cfg := &config.Config{
		DynamoDB: config.DynamoDBConfig{
			Provisioned: config.ProvisionedConfig{
				PerTableMaxRCU: 50000,
				PerTableMaxWCU: 50000,
			},
		},
	}
	svc := NewDynamoDBService(cfg)

	assert.Equal(t, int64(50000), svc.GetProvisionedMaxRCU())
	assert.Equal(t, int64(50000), svc.GetProvisionedMaxWCU())
}

func TestDynamoDBService_ParseBatchWriteItem(t *testing.T) {
	cfg := &config.Config{}
	svc := NewDynamoDBService(cfg)

	tests := []struct {
		name          string
		body          string
		expectedCount int
		expectError   bool
	}{
		{
			"Single table with 3 items",
			`{"RequestItems":{"table1":[{"PutRequest":{"Item":{"id":{"S":"1"}}}},{"PutRequest":{"Item":{"id":{"S":"2"}}}},{"PutRequest":{"Item":{"id":{"S":"3"}}}}]}}`,
			3,
			false,
		},
		{
			"Multiple tables",
			`{"RequestItems":{"table1":[{"PutRequest":{"Item":{"id":{"S":"1"}}}}],"table2":[{"PutRequest":{"Item":{"id":{"S":"2"}}}},{"PutRequest":{"Item":{"id":{"S":"3"}}}}]}}`,
			0,
			false,
		},
		{
			"Empty request",
			`{"RequestItems":{}}`,
			0,
			false,
		},
		{
			"Invalid JSON",
			`{invalid}`,
			0,
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := svc.ParseBatchWriteItem([]byte(tt.body))
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tt.expectedCount > 0 {
					count := result["table1"]
					assert.Equal(t, tt.expectedCount, count)
				}
			}
		})
	}
}

func TestDynamoDBService_ParseBatchGetItem(t *testing.T) {
	cfg := &config.Config{}
	svc := NewDynamoDBService(cfg)

	tests := []struct {
		name          string
		body          string
		expectedCount int
		expectError   bool
	}{
		{
			"Single table with 5 keys",
			`{"RequestItems":{"table1":[{"Keys":{"id":{"S":"1"}}},{"Keys":{"id":{"S":"2"}}},{"Keys":{"id":{"S":"3"}}},{"Keys":{"id":{"S":"4"}}},{"Keys":{"id":{"S":"5"}}}]}}`,
			5,
			false,
		},
		{
			"Multiple tables",
			`{"RequestItems":{"table1":[{"Keys":{"id":{"S":"1"}}}],"table2":[{"Keys":{"id":{"S":"2"}}},{"Keys":{"id":{"S":"3"}}}]}}`,
			0,
			false,
		},
		{
			"Empty request",
			`{"RequestItems":{}}`,
			0,
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := svc.ParseBatchGetItem([]byte(tt.body))
			assert.NoError(t, err)
			if tt.expectedCount > 0 {
				count := result["table1"]
				assert.Equal(t, tt.expectedCount, count)
			}
		})
	}
}

func TestDynamoDBService_CalculateBatchWriteWCU(t *testing.T) {
	cfg := &config.Config{}
	svc := NewDynamoDBService(cfg)

	tests := []struct {
		name           string
		tableName      string
		body           string
		expectedWCUMin int64
	}{
		{
			"3 PutRequest items",
			"test-table",
			`{"RequestItems":{"test-table":[{"PutRequest":{"Item":{"id":{"S":"1"},"data":{"S":"short"}}}},{"PutRequest":{"Item":{"id":{"S":"2"},"data":{"S":"short"}}}},{"PutRequest":{"Item":{"id":{"S":"3"},"data":{"S":"short"}}}}]}}`,
			3,
		},
		{
			"Mixed Put and Delete",
			"test-table",
			`{"RequestItems":{"test-table":[{"PutRequest":{"Item":{"id":{"S":"1"}}}},{"DeleteRequest":{"Key":{"id":{"S":"2"}}}}]}}`,
			2,
		},
		{
			"Empty table",
			"other-table",
			`{"RequestItems":{"test-table":[{"PutRequest":{"Item":{"id":{"S":"1"}}}}]}}`,
			1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wcu := svc.CalculateBatchWriteWCU(tt.tableName, []byte(tt.body))
			assert.GreaterOrEqual(t, wcu, tt.expectedWCUMin)
		})
	}
}

func TestDynamoDBService_CalculateBatchGetRCU(t *testing.T) {
	cfg := &config.Config{}
	svc := NewDynamoDBService(cfg)

	tests := []struct {
		name           string
		tableName      string
		body           string
		expectedRCUMin int64
	}{
		{
			"5 keys eventually consistent",
			"test-table",
			`{"RequestItems":{"test-table":[{"Keys":{"id":{"S":"1"}}},{"Keys":{"id":{"S":"2"}}},{"Keys":{"id":{"S":"3"}}},{"Keys":{"id":{"S":"4"}}},{"Keys":{"id":{"S":"5"}}}]}}`,
			5,
		},
		{
			"Empty table",
			"other-table",
			`{"RequestItems":{"test-table":[{"Keys":{"id":{"S":"1"}}}]}}`,
			1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rcu := svc.CalculateBatchGetRCU(tt.tableName, []byte(tt.body), false)
			assert.GreaterOrEqual(t, rcu, tt.expectedRCUMin)
		})
	}
}