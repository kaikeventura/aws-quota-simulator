package services

import (
	"encoding/json"
	"strings"
	"sync"

	"github.com/aws-quota-simulator/internal/config"
)

type BillingMode string

const (
	BillingModeProvisioned   BillingMode = "PROVISIONED"
	BillingModeOnDemand      BillingMode = "PAY_PER_REQUEST"
)

type DynamoDBService struct {
	cfg           *config.Config
	tables        map[string]*TableMetadata
	mu            sync.RWMutex
	accountUsedRCU int64
	accountUsedWCU int64
	accountMu     sync.Mutex
}

type TableMetadata struct {
	TableName      string
	BillingMode    BillingMode
	ProvisionedRCU int64
	ProvisionedWCU int64
}

func NewDynamoDBService(cfg *config.Config) *DynamoDBService {
	return &DynamoDBService{
		cfg:    cfg,
		tables: make(map[string]*TableMetadata),
	}
}

type DynamoDBRequest struct {
	TableName             string                  `json:"TableName,omitempty"`
	ProvisionedThroughput *ProvisionedThroughput  `json:"ProvisionedThroughput,omitempty"`
	BillingMode          string                  `json:"BillingMode,omitempty"`
	KeyConditionExpression string                `json:"KeyConditionExpression,omitempty"`
	ScanFilter           map[string]Condition    `json:"ScanFilter,omitempty"`
	ConditionExpression  string                  `json:"ConditionExpression,omitempty"`
	UpdateExpression     string                  `json:"UpdateExpression,omitempty"`
	PutItem              map[string]AttributeValue `json:"PutItem,omitempty"`
	GetItem              map[string]AttributeValue `json:"GetItem,omitempty"`
	DeleteItem           map[string]AttributeValue `json:"DeleteItem,omitempty"`
	Item                 map[string]AttributeValue `json:"Item,omitempty"`
	Key                  map[string]AttributeValue `json:"Key,omitempty"`
}

type ProvisionedThroughput struct {
	ReadCapacityUnits  int64 `json:"ReadCapacityUnits,omitempty"`
	WriteCapacityUnits int64 `json:"WriteCapacityUnits,omitempty"`
}

type Condition struct {
	ComparisonOperator  string            `json:"ComparisonOperator,omitempty"`
	AttributeValueList   []AttributeValue  `json:"AttributeValueList,omitempty"`
}

type AttributeValue struct {
	S string `json:"S,omitempty"`
	N string `json:"N,omitempty"`
	B string `json:"B,omitempty"`
	Bm string `json:"BOOL,omitempty"`
}

func (d *DynamoDBService) CheckLimit(service, operation, resource string) error {
	return nil
}

func (d *DynamoDBService) GetRateLimit(service, resource string) (rate, burst int64) {
	return d.cfg.DynamoDB.OnDemand.MaxTableReadRPS, d.cfg.DynamoDB.OnDemand.MaxTableReadRPS * 2
}

func (d *DynamoDBService) ParseTableName(body []byte) string {
	var req DynamoDBRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return ""
	}
	return req.TableName
}

func (d *DynamoDBService) DetectOperation(path, action string) (operation, table string) {
	path = strings.ToLower(path)
	actionLower := strings.ToLower(action)

	checkStr := path
	if action != "" {
		checkStr = actionLower
	}

	if strings.Contains(checkStr, "putitem") {
		return "put_item", ""
	}
	if strings.Contains(checkStr, "getitem") {
		return "get_item", ""
	}
	if strings.Contains(checkStr, "deleteitem") {
		return "delete_item", ""
	}
	if strings.Contains(checkStr, "updateitem") {
		return "update_item", ""
	}
	if strings.Contains(checkStr, "query") && !strings.Contains(checkStr, "batch") {
		return "query", ""
	}
	if strings.Contains(checkStr, "scan") {
		return "scan", ""
	}
	if strings.Contains(checkStr, "createtable") {
		return "create_table", ""
	}
	if strings.Contains(checkStr, "describelimits") {
		return "describe_limits", ""
	}
	if strings.Contains(checkStr, "batchgetitem") {
		return "batch_get_item", ""
	}
	if strings.Contains(checkStr, "batchwriteitem") {
		return "batch_write_item", ""
	}

	return "", ""
}

func (d *DynamoDBService) IsReadOperation(operation string) bool {
	readOps := map[string]bool{
		"get_item":       true,
		"query":          true,
		"scan":           true,
		"batch_get_item": true,
	}
	return readOps[operation]
}

func (d *DynamoDBService) IsWriteOperation(operation string) bool {
	writeOps := map[string]bool{
		"put_item":          true,
		"delete_item":       true,
		"update_item":       true,
		"create_table":      true,
		"batch_write_item":  true,
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

func (d *DynamoDBService) GetOnDemandMaxReadRPS() int64 {
	return d.cfg.DynamoDB.OnDemand.MaxTableReadRPS
}

func (d *DynamoDBService) GetOnDemandMaxWriteRPS() int64 {
	return d.cfg.DynamoDB.OnDemand.MaxTableWriteRPS
}

func (d *DynamoDBService) GetOnDemandInitialReadRPS() int64 {
	return d.cfg.DynamoDB.OnDemand.InitialReadRPS
}

func (d *DynamoDBService) GetOnDemandInitialWriteRPS() int64 {
	return d.cfg.DynamoDB.OnDemand.InitialWriteRPS
}

func (d *DynamoDBService) GetProvisionedMaxRCU() int64 {
	return d.cfg.DynamoDB.Provisioned.PerTableMaxRCU
}

func (d *DynamoDBService) GetProvisionedMaxWCU() int64 {
	return d.cfg.DynamoDB.Provisioned.PerTableMaxWCU
}

func (d *DynamoDBService) ParseCreateTableRequest(body []byte) (tableName string, billingMode BillingMode, rcu, wcu int64) {
	var req DynamoDBRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return "", "", 0, 0
	}

	tableName = req.TableName

	if strings.ToUpper(req.BillingMode) == "PAY_PER_REQUEST" {
		billingMode = BillingModeOnDemand
	} else {
		billingMode = BillingModeProvisioned
	}

	if req.ProvisionedThroughput != nil {
		rcu = req.ProvisionedThroughput.ReadCapacityUnits
		wcu = req.ProvisionedThroughput.WriteCapacityUnits
	}

	return tableName, billingMode, rcu, wcu
}

func (d *DynamoDBService) RegisterTable(tableName string, mode BillingMode, rcu, wcu int64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if mode == BillingModeOnDemand {
		rcu = d.cfg.DynamoDB.OnDemand.InitialReadRPS
		wcu = d.cfg.DynamoDB.OnDemand.InitialWriteRPS
	}

	d.tables[tableName] = &TableMetadata{
		TableName:      tableName,
		BillingMode:    mode,
		ProvisionedRCU: rcu,
		ProvisionedWCU: wcu,
	}
}

func (d *DynamoDBService) GetTableMetadata(tableName string) (*TableMetadata, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	meta, ok := d.tables[tableName]
	return meta, ok
}

func (d *DynamoDBService) GetBillingMode(tableName string) BillingMode {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if meta, ok := d.tables[tableName]; ok {
		return meta.BillingMode
	}
	return BillingModeOnDemand
}

func (d *DynamoDBService) CalculateRCU(operation string, itemSize int64) int64 {
	switch operation {
	case "get_item":
		if itemSize <= 0 {
			itemSize = 4096
		}
		return (itemSize + 4095) / 4096 * 1
	case "query":
		if itemSize <= 0 {
			itemSize = 4096
		}
		return (itemSize + 4095) / 4096
	case "scan":
		if itemSize <= 0 {
			itemSize = 4096
		}
		return (itemSize + 4095) / 4096
	default:
		return 1
	}
}

func (d *DynamoDBService) CalculateWCU(operation string, itemSize int64) int64 {
	switch operation {
	case "put_item":
		if itemSize <= 0 {
			itemSize = 1024
		}
		return (itemSize + 1023) / 1024
	case "delete_item":
		if itemSize <= 0 {
			itemSize = 1024
		}
		return (itemSize + 1023) / 1024
	case "update_item":
		if itemSize <= 0 {
			itemSize = 1024
		}
		return (itemSize + 1023) / 1024
	default:
		return 1
	}
}

func (d *DynamoDBService) EstimateItemSize(body []byte) int64 {
	var req DynamoDBRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return 1024
	}

	size := int64(0)

	if req.Item != nil {
		size += estimateMapSize(req.Item)
	}
	if req.PutItem != nil {
		size += estimateMapSize(req.PutItem)
	}
	if req.GetItem != nil {
		size += estimateMapSize(req.GetItem)
	}
	if req.DeleteItem != nil {
		size += estimateMapSize(req.DeleteItem)
	}

	if size == 0 {
		return 1024
	}
	return size
}

func estimateMapSize(m map[string]AttributeValue) int64 {
	var size int64 = 0
	for _, v := range m {
		size += int64(len(v.S) + len(v.N) + len(v.B))
	}
	return size
}

func (d *DynamoDBService) GetAccountUsedRCU() int64 {
	d.accountMu.Lock()
	defer d.accountMu.Unlock()
	return d.accountUsedRCU
}

func (d *DynamoDBService) GetAccountUsedWCU() int64 {
	d.accountMu.Lock()
	defer d.accountMu.Unlock()
	return d.accountUsedWCU
}

func (d *DynamoDBService) ConsumeAccountRCU(rcu int64) {
	d.accountMu.Lock()
	defer d.accountMu.Unlock()
	d.accountUsedRCU += rcu
}

func (d *DynamoDBService) ConsumeAccountWCU(wcu int64) {
	d.accountMu.Lock()
	defer d.accountMu.Unlock()
	d.accountUsedWCU += wcu
}

func (d *DynamoDBService) ReleaseAccountRCU(rcu int64) {
	d.accountMu.Lock()
	defer d.accountMu.Unlock()
	d.accountUsedRCU -= rcu
	if d.accountUsedRCU < 0 {
		d.accountUsedRCU = 0
	}
}

func (d *DynamoDBService) ReleaseAccountWCU(wcu int64) {
	d.accountMu.Lock()
	defer d.accountMu.Unlock()
	d.accountUsedWCU -= wcu
	if d.accountUsedWCU < 0 {
		d.accountUsedWCU = 0
	}
}

type BatchWriteItemRequest struct {
	RequestItems map[string][]WriteRequest `json:"RequestItems,omitempty"`
}

type WriteRequest struct {
	PutRequest *PutRequest `json:"PutRequest,omitempty"`
	DeleteRequest *DeleteRequest `json:"DeleteRequest,omitempty"`
}

type PutRequest struct {
	Item map[string]AttributeValue `json:"Item,omitempty"`
}

type DeleteRequest struct {
	Key map[string]AttributeValue `json:"Key,omitempty"`
}

type BatchGetItemRequest struct {
	RequestItems map[string][]map[string]interface{} `json:"RequestItems,omitempty"`
}

func (d *DynamoDBService) ParseBatchWriteItem(body []byte) (map[string]int, error) {
	var req BatchWriteItemRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	result := make(map[string]int)
	for tableName, writes := range req.RequestItems {
		result[tableName] = len(writes)
	}
	return result, nil
}

func (d *DynamoDBService) ParseBatchGetItem(body []byte) (map[string]int, error) {
	var req BatchGetItemRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	result := make(map[string]int)
	for tableName, items := range req.RequestItems {
		result[tableName] = len(items)
	}
	return result, nil
}

func (d *DynamoDBService) CalculateBatchWriteWCU(tableName string, body []byte) int64 {
	var req BatchWriteItemRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return 1
	}

	writes, ok := req.RequestItems[tableName]
	if !ok || len(writes) == 0 {
		return 1
	}

	var totalWCU int64 = 0
	for _, write := range writes {
		var itemSize int64 = 1024

		if write.PutRequest != nil && write.PutRequest.Item != nil {
			itemSize = estimateMapSize(write.PutRequest.Item)
		} else if write.DeleteRequest != nil && write.DeleteRequest.Key != nil {
			itemSize = estimateMapSize(write.DeleteRequest.Key)
		}

		wcu := (itemSize + 1023) / 1024
		if wcu < 1 {
			wcu = 1
		}
		totalWCU += wcu
	}

	return totalWCU
}

func (d *DynamoDBService) CalculateBatchGetRCU(tableName string, body []byte, consistentRead bool) int64 {
	var req BatchGetItemRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return 1
	}

	items, ok := req.RequestItems[tableName]
	if !ok || len(items) == 0 {
		return 1
	}

	var totalRCU int64 = 0
	rcuMultiplier := int64(1)
	if !consistentRead {
		rcuMultiplier = 2
	}

	for _, item := range items {
		var itemSize int64 = 4096

		if keysObj, ok := item["Keys"]; ok {
			if keysMap, ok := keysObj.(map[string]interface{}); ok {
				itemSize = estimateInterfaceMapSize(keysMap)
			}
		}

		rcu := (itemSize + 4095) / 4096
		if rcu < 1 {
			rcu = 1
		}

		totalRCU += (rcu * rcuMultiplier)
	}

	return totalRCU
}

func estimateInterfaceMapSize(m map[string]interface{}) int64 {
	var size int64 = 0
	for _, v := range m {
		switch val := v.(type) {
		case map[string]interface{}:
			size += estimateInterfaceMapSize(val)
		case string:
			size += int64(len(val))
		}
	}
	if size == 0 {
		return 4096
	}
	return size
}