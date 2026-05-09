package services

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/aws-quota-simulator/internal/config"
)

type SQSService struct {
	cfg *config.Config
}

func NewSQSService(cfg *config.Config) *SQSService {
	return &SQSService{cfg: cfg}
}

type SQSRequest struct {
	QueueUrl       string `json:"QueueUrl,omitempty"`
	QueueName      string `json:"QueueName,omitempty"`
	MaxNumberOfMessages *int64 `json:"MaxNumberOfMessages,omitempty"`
}

type SQSBatchRequest struct {
	QueueUrl string `json:"QueueUrl,omitempty"`
	Entries  []struct {
		Id string `json:"Id,omitempty"`
	} `json:"Entries,omitempty"`
}

func (s *SQSService) CheckLimit(service, operation, resource string) error {
	return nil // SQS uses rate limiter, not this method
}

func (s *SQSService) GetRateLimit(service, resource string) (rate, burst int64) {
	return s.cfg.SQS.FIFO.TPSLimit, s.cfg.SQS.FIFO.BurstLimit
}

func (s *SQSService) IsFIFOQueue(queueURL string) bool {
	return strings.HasSuffix(queueURL, ".fifo")
}

func (s *SQSService) IsBatchOperation(body []byte) bool {
	var batchReq SQSBatchRequest
	if err := json.Unmarshal(body, &batchReq); err != nil {
		return false
	}
	return len(batchReq.Entries) > 1
}

func (s *SQSService) ParseQueueURL(body []byte) string {
	var req SQSRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return ""
	}
	return req.QueueUrl
}

func (s *SQSService) GetMessageSize(body []byte) int64 {
	var req struct {
		MessageBody string `json:"MessageBody,omitempty"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return int64(len(body))
	}
	return int64(len(req.MessageBody))
}

func (s *SQSService) GetBatchEntryCount(body []byte) int {
	var req SQSBatchRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return 0
	}
	return len(req.Entries)
}

func (s *SQSService) ParseMessageDeduplicationID(body []byte) string {
	var req struct {
		MessageDeduplicationId string `json:"MessageDeduplicationId,omitempty"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return ""
	}
	return req.MessageDeduplicationId
}

func (s *SQSService) CalculateContentBasedDedupID(body []byte) string {
	var req struct {
		MessageBody string `json:"MessageBody,omitempty"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		hash := sha256.Sum256(body)
		return hex.EncodeToString(hash[:])
	}

	hash := sha256.Sum256([]byte(req.MessageBody))
	return hex.EncodeToString(hash[:])
}

func (s *SQSService) DetectOperation(path, body string) (operation, resource string) {
	pathLower := strings.ToLower(path)
	bodyLower := strings.ToLower(body)

	if strings.Contains(pathLower, "sendmessage") || strings.Contains(bodyLower, "sendmessage") {
		if strings.Contains(pathLower, "batch") || strings.Contains(bodyLower, "sendmessagebatch") {
			return "send_batch", ""
		}
		return "send", ""
	}
	if strings.Contains(pathLower, "receivemessage") || strings.Contains(bodyLower, "receivemessage") {
		return "receive", ""
	}
	if strings.Contains(pathLower, "deletemessage") || strings.Contains(bodyLower, "deletemessage") {
		if strings.Contains(pathLower, "batch") || strings.Contains(bodyLower, "deletemessagebatch") {
			return "delete_batch", ""
		}
		return "delete", ""
	}
	if strings.Contains(pathLower, "createqueue") || strings.Contains(bodyLower, "createqueue") {
		return "create", ""
	}
	if strings.Contains(pathLower, "listqueues") || strings.Contains(bodyLower, "listqueues") {
		return "list", ""
	}

	return "", ""
}

func (s *SQSService) DetectOperationFromAction(action, body string) (operation, resource string) {
	actionLower := strings.ToLower(action)
	bodyLower := strings.ToLower(body)

	if strings.Contains(actionLower, "sendmessage") {
		if strings.Contains(actionLower, "batch") || strings.Contains(bodyLower, "sendmessagebatch") {
			return "send_batch", ""
		}
		return "send", ""
	}
	if strings.Contains(actionLower, "receivemessage") {
		return "receive", ""
	}
	if strings.Contains(actionLower, "deletemessage") {
		if strings.Contains(actionLower, "batch") || strings.Contains(bodyLower, "deletemessagebatch") {
			return "delete_batch", ""
		}
		return "delete", ""
	}
	if strings.Contains(actionLower, "createqueue") {
		return "create", ""
	}
	if strings.Contains(actionLower, "listqueues") {
		return "list", ""
	}

	return "", ""
}

func (s *SQSService) ShouldThrottle(operation string, isFIFO, isBatch bool) (shouldThrottle bool, tpsLimit int64) {
	if isFIFO {
		switch operation {
		case "send":
			return true, s.cfg.SQS.FIFO.TPSLimit
		case "receive":
			return true, s.cfg.SQS.FIFO.ReceiveTPSLimit
		case "delete":
			return true, s.cfg.SQS.FIFO.DeleteTPSLimit
		case "send_batch", "delete_batch":
			return true, s.cfg.SQS.FIFO.BatchTPSLimit
		default:
			return false, 0
		}
	}

	switch operation {
	case "send":
		return true, s.cfg.SQS.Standard.SendTPSLimit
	case "receive":
		return true, s.cfg.SQS.Standard.ReceiveTPSLimit
	case "delete":
		return true, s.cfg.SQS.Standard.DeleteTPSLimit
	case "send_batch", "delete_batch":
		return true, s.cfg.SQS.Standard.BatchTPSLimit
	default:
		return false, 0
	}
}