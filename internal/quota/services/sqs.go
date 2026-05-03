package services

import (
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
	if !isFIFO {
		// Standard queues have very high limits
		return false, s.cfg.SQS.Standard.RateLimit
	}

	switch operation {
	case "send", "receive", "delete":
		if isBatch {
			return true, s.cfg.SQS.FIFO.BatchTPSLimit
		}
		return true, s.cfg.SQS.FIFO.TPSLimit
	case "send_batch", "delete_batch":
		return true, s.cfg.SQS.FIFO.BatchTPSLimit
	default:
		return false, 0
	}
}