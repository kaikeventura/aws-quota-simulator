package services

import (
	"encoding/json"
	"strings"

	"github.com/aws-quota-simulator/internal/config"
)

type DynamoDBService struct {
	cfg *config.Config
}

func NewDynamoDBService(cfg *config.Config) *DynamoDBService {
	return &DynamoDBService{cfg: cfg}
}

type DynamoDBRequest struct {
	TableName            string `json:"TableName,omitempty"`
	ProvisionedThroughput *ProvisionedThroughput `json:"ProvisionedThroughput,omitempty"`
	BillingMode          string `json:"BillingMode,omitempty"`
	KeyConditionExpression string `json:"KeyConditionExpression,omitempty"`
	ScanFilter           map[string]Condition `json:"ScanFilter,omitempty"`
	ConditionExpression  string `json:"ConditionExpression,omitempty"`
	UpdateExpression     string `json:"UpdateExpression,omitempty"`
	PutItem              map[string]AttributeValue `json:"PutItem,omitempty"`
	GetItem              map[string]AttributeValue `json:"GetItem,omitempty"`
	DeleteItem           map[string]AttributeValue `json:"DeleteItem,omitempty"`
}

type ProvisionedThroughput struct {
	ReadCapacityUnits  int64 `json:"ReadCapacityUnits,omitempty"`
	WriteCapacityUnits int64 `json:"WriteCapacityUnits,omitempty"`
}

type Condition struct {
	ComparisonOperator string `json:"ComparisonOperator,omitempty"`
	AttributeValueList []AttributeValue `json:"AttributeValueList,omitempty"`
}

type AttributeValue struct {
	S string `json:"S,omitempty"`
	N string `json:"N,omitempty"`
	B string `json:"B,omitempty"`
}

func (d *DynamoDBService) CheckLimit(service, operation, resource string) error {
	return nil // DynamoDB uses rate limiter for throughput
}

func (d *DynamoDBService) GetRateLimit(service, resource string) (rate, burst int64) {
	return d.cfg.DynamoDB.OnDemand.MaxTableRPS, d.cfg.DynamoDB.OnDemand.MaxTableRPS * 2
}

func (d *DynamoDBService) ParseTableName(body []byte) string {
	var req DynamoDBRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return ""
	}
	return req.TableName
}

func (d *DynamoDBService) DetectOperation(path string) (operation, table string) {
	path = strings.ToLower(path)

	if strings.Contains(path, "putitem") {
		return "put_item", ""
	}
	if strings.Contains(path, "getitem") {
		return "get_item", ""
	}
	if strings.Contains(path, "deleteitem") {
		return "delete_item", ""
	}
	if strings.Contains(path, "updateitem") {
		return "update_item", ""
	}
	if strings.Contains(path, "query") {
		return "query", ""
	}
	if strings.Contains(path, "scan") {
		return "scan", ""
	}
	if strings.Contains(path, "createtable") {
		return "create_table", ""
	}
	if strings.Contains(path, "describelimits") {
		return "describe_limits", ""
	}

	return "", ""
}

func (d *DynamoDBService) IsReadOperation(operation string) bool {
	readOps := map[string]bool{
		"get_item": true,
		"query":    true,
		"scan":     true,
	}
	return readOps[operation]
}

func (d *DynamoDBService) IsWriteOperation(operation string) bool {
	writeOps := map[string]bool{
		"put_item":    true,
		"delete_item": true,
		"update_item": true,
		"create_table": true,
	}
	return writeOps[operation]
}

func (d *DynamoDBService) GetDefaultRCU() int64 {
	return d.cfg.DynamoDB.Default.TableMaxRCU
}

func (d *DynamoDBService) GetDefaultWCU() int64 {
	return d.cfg.DynamoDB.Default.TableMaxWCU
}

func (d *DynamoDBService) GetAccountMaxRCU() int64 {
	return d.cfg.DynamoDB.Provisioned.AccountMaxRCU
}

func (d *DynamoDBService) GetAccountMaxWCU() int64 {
	return d.cfg.DynamoDB.Provisioned.AccountMaxWCU
}

func (d *DynamoDBService) GetOnDemandMaxRPS() int64 {
	return d.cfg.DynamoDB.OnDemand.MaxTableRPS
}