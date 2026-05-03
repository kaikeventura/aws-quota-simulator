package errors

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAWSError_New(t *testing.T) {
	err := NewAWSError("RequestThrottled", "Rate limit exceeded", true)

	assert.Equal(t, "com.amazonaws.sqs.v#RequestThrottled", err.Type)
	assert.Equal(t, "Rate limit exceeded", err.Message)
	assert.True(t, err.Retryable)
}

func TestAWSError_ProvisionedThroughput(t *testing.T) {
	err := NewAWSError("ProvisionedThroughputExceededException", "Rate exceeded for table: users", true)

	assert.Equal(t, "com.amazonaws.dynamodb.v#ProvisionedThroughputExceededException", err.Type)
	assert.Equal(t, "Rate exceeded for table: users", err.Message)
	assert.True(t, err.Retryable)
}

func TestAWSError_Code(t *testing.T) {
	err := NewAWSError("RequestThrottled", "test", false)
	assert.Equal(t, "RequestThrottled", err.Code())

	err2 := NewAWSError("ThrottlingException", "test", false)
	assert.Equal(t, "ThrottlingException", err2.Code())
}

func TestAWSError_Error(t *testing.T) {
	err := NewAWSError("RequestThrottled", "message", true)
	assert.Equal(t, "[com.amazonaws.sqs.v#RequestThrottled] message", err.Error())
}

func TestAWSError_ToJSON(t *testing.T) {
	awsErr := NewAWSError("RequestThrottled", "test message", true)
	jsonData := awsErr.ToJSON()

	var parsed map[string]interface{}
	err := json.Unmarshal(jsonData, &parsed)
	assert.NoError(t, err)

	assert.Equal(t, "com.amazonaws.sqs.v#RequestThrottled", parsed["__type"])
	assert.Equal(t, "test message", parsed["message"])
	assert.Equal(t, true, parsed["retryable"])
}

func TestSendThrottlingError_SQS(t *testing.T) {
	w := httptest.NewRecorder()
	SendThrottlingError(w, "sqs", "Rate limit exceeded for queue")

	assert.Equal(t, 400, w.Code)
	assert.Equal(t, "application/x-amz-json-1.0", w.Header().Get("Content-Type"))

	var parsed map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &parsed)
	assert.NoError(t, err)
	assert.Contains(t, parsed["__type"], "RequestThrottled")
}

func TestSendThrottlingError_DynamoDB(t *testing.T) {
	w := httptest.NewRecorder()
	SendThrottlingError(w, "dynamodb", "Rate exceeded for table")

	assert.Equal(t, 400, w.Code)

	var parsed map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &parsed)
	assert.NoError(t, err)
	assert.Contains(t, parsed["__type"], "ProvisionedThroughputExceededException")
}

func TestSendLimitExceededError(t *testing.T) {
	w := httptest.NewRecorder()
	SendLimitExceededError(w, "dynamodb", "Account limit exceeded")

	assert.Equal(t, 400, w.Code)

	var parsed map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &parsed)
	assert.NoError(t, err)
	assert.Contains(t, parsed["__type"], "LimitExceededException")
	_, hasRetryable := parsed["retryable"]
	assert.False(t, hasRetryable, "retryable should be omitted when false")
}