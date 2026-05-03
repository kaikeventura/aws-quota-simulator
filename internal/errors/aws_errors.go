package errors

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

type AWSError struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
	Retryable bool  `json:"retryable,omitempty"`
}

func NewAWSError(code, message string, retryable bool) *AWSError {
	typePrefix := map[string]string{
		"RequestThrottled":                    "com.amazonaws.sqs.v#RequestThrottled",
		"ProvisionedThroughputExceededException": "com.amazonaws.dynamodb.v#ProvisionedThroughputExceededException",
		"ThrottlingException":                 "com.amazonaws.sdk.v1#ThrottlingException",
		"LimitExceededException":              "com.amazonaws.dynamodb.v#LimitExceededException",
	}

	prefix, ok := typePrefix[code]
	if !ok {
		prefix = "com.amazonaws.sdk.v1#" + code
	}

	return &AWSError{
		Type:      prefix,
		Message:   message,
		Retryable: retryable,
	}
}

func (e *AWSError) Error() string {
	return fmt.Sprintf("[%s] %s", e.Type, e.Message)
}

func (e *AWSError) Code() string {
	parts := strings.Split(e.Type, "#")
	if len(parts) > 1 {
		return parts[1]
	}
	return e.Type
}

func (e *AWSError) ToJSON() []byte {
	data, _ := json.Marshal(e)
	return data
}

func SendThrottlingError(w http.ResponseWriter, service, message string) {
	var awsErr *AWSError

	switch service {
	case "sqs":
		awsErr = NewAWSError("RequestThrottled", message, true)
	case "dynamodb":
		awsErr = NewAWSError("ProvisionedThroughputExceededException", message, true)
	default:
		awsErr = NewAWSError("ThrottlingException", message, true)
	}

	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.Header().Set("x-amzn-RequestId", generateRequestID())
	w.WriteHeader(http.StatusBadRequest)
	w.Write(awsErr.ToJSON())
}

func SendLimitExceededError(w http.ResponseWriter, service, message string) {
	awsErr := NewAWSError("LimitExceededException", message, false)

	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.Header().Set("x-amzn-RequestId", generateRequestID())
	w.WriteHeader(http.StatusBadRequest)
	w.Write(awsErr.ToJSON())
}

func generateRequestID() string {
	return "00000000-0000-0000-0000-000000000000"
}