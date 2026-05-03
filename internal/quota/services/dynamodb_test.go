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
				MaxTableRPS: 40000,
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
			operation, _ := svc.DetectOperation(tt.path)
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
				MaxTableRPS:        40000,
				AccountMaxReadRPS:  40000,
				AccountMaxWriteRPS: 40000,
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

	assert.Equal(t, int64(40000), svc.GetOnDemandMaxRPS())
	assert.Equal(t, int64(80000), svc.GetAccountMaxRCU())
	assert.Equal(t, int64(80000), svc.GetAccountMaxWCU())
	assert.Equal(t, int64(10000), svc.GetDefaultRCU())
	assert.Equal(t, int64(10000), svc.GetDefaultWCU())
}