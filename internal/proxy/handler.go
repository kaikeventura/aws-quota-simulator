package proxy

import (
	"bytes"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/aws-quota-simulator/internal/errors"
	"github.com/aws-quota-simulator/internal/quota"
	"github.com/aws-quota-simulator/internal/quota/services"
)

type ProxyHandler struct {
	reverseProxy *httputil.ReverseProxy
	quotaManager *quota.QuotaManager
}

func NewProxyHandler(upstreamURL string, quotaManager *quota.QuotaManager) (*ProxyHandler, error) {
	upstream, err := url.Parse(upstreamURL)
	if err != nil {
		return nil, err
	}

	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 10 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		DisableKeepAlives:     false,
	}

	proxy := httputil.NewSingleHostReverseProxy(upstream)
	proxy.Transport = transport
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		errStr := err.Error()
		if strings.Contains(errStr, "context canceled") || strings.Contains(errStr, "client disconnected") {
			log.Printf("Client disconnected: %s %s", r.Method, r.URL.Path)
			return
		}
		log.Printf("Proxy error for %s %s: %v", r.Method, r.URL.Path, err)
		http.Error(w, "Upstream unavailable: "+err.Error(), http.StatusBadGateway)
	}

	return &ProxyHandler{
		reverseProxy: proxy,
		quotaManager: quotaManager,
	}, nil
}

func (h *ProxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	query := r.URL.RawQuery
	host := r.Host
	action := r.Header.Get("X-Amz-Target")

	service := detectService(path)
	if service == "" {
		service = detectServiceFromHost(host)
	}
	if service == "" {
		service = detectServiceFromQuery(query)
	}

	log.Printf("Request: %s %s?%s Host:%s Action:%s Service:%s", r.Method, path, query, host, action, service)

	if service == "sqs" || service == "dynamodb" {
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil && err != io.EOF {
			log.Printf("Error reading body: %v", err)
		}

		if len(bodyBytes) > 0 {
			allowed, message := h.checkQuota(service, path, query, action, bodyBytes)
			if !allowed {
				log.Printf("Quota exceeded for %s: %s", service, message)
				errors.SendThrottlingError(w, service, message)
				return
			}

			r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
			r.ContentLength = int64(len(bodyBytes))
		}
	}

	h.reverseProxy.ServeHTTP(w, r)
}

func (h *ProxyHandler) checkQuota(service, path, query, action string, body []byte) (bool, string) {
	switch service {
	case "sqs":
		return h.checkSQSQuota(path, query, action, body)
	case "dynamodb":
		return h.checkDynamoDBQuota(path, body)
	default:
		return true, ""
	}
}

func (h *ProxyHandler) checkSQSQuota(path, query, action string, body []byte) (bool, string) {
	sqsSvcRaw, ok := h.quotaManager.GetService("sqs")
	if !ok {
		return true, ""
	}
	sqsSvc := sqsSvcRaw.(*services.SQSService)

	operation, _ := sqsSvc.DetectOperationFromAction(action, string(body))
	log.Printf("SQS check - operation: %s, action: %s, body len: %d", operation, action, len(body))
	if operation == "" {
		return true, ""
	}

	queueURL := sqsSvc.ParseQueueURL(body)
	if queueURL == "" {
		queueURL = "default"
	}
	isFIFO := sqsSvc.IsFIFOQueue(queueURL)
	isBatch := sqsSvc.IsBatchOperation(body)

	log.Printf("SQS - queue: %s, FIFO: %v, batch: %v", queueURL, isFIFO, isBatch)

	shouldThrottle, tpsLimit := sqsSvc.ShouldThrottle(operation, isFIFO, isBatch)
	if !shouldThrottle {
		return true, ""
	}

	limiter := h.quotaManager.GetLimiter("sqs", queueURL)
	if limiter == nil {
		return true, ""
	}

	bucket := limiter.GetOrCreate(queueURL, tpsLimit, tpsLimit)
	if bucket.Allow() {
		return true, ""
	}

	return false, "Rate exceeded for queue: " + queueURL
}

func (h *ProxyHandler) checkDynamoDBQuota(path string, body []byte) (bool, string) {
	dynamoSvcRaw, ok := h.quotaManager.GetService("dynamodb")
	if !ok {
		return true, ""
	}
	dynamoSvc := dynamoSvcRaw.(*services.DynamoDBService)

	operation, table := dynamoSvc.DetectOperation(path)
	if operation == "" {
		return true, ""
	}

	if table == "" {
		table = dynamoSvc.ParseTableName(body)
	}

	allowed, _ := h.quotaManager.CheckRateLimit("dynamodb", table)
	if !allowed {
		return false, "Rate exceeded for table: " + table
	}

	return true, ""
}

func detectService(path string) string {
	pathLower := strings.ToLower(path)

	if strings.Contains(pathLower, "sqs") || strings.Contains(pathLower, "amazonaws.com/sqs") {
		return "sqs"
	}
	if strings.Contains(pathLower, "dynamodb") || strings.Contains(pathLower, "amazonaws.com/dynamodb") {
		return "dynamodb"
	}

	return ""
}

func detectServiceFromHost(host string) string {
	hostLower := strings.ToLower(host)
	if strings.Contains(hostLower, "sqs") {
		return "sqs"
	}
	if strings.Contains(hostLower, "dynamodb") {
		return "dynamodb"
	}
	if strings.Contains(hostLower, "localhost:4567") {
		return "sqs"
	}
	return ""
}

func detectServiceFromQuery(query string) string {
	queryLower := strings.ToLower(query)
	if strings.Contains(queryLower, "sendmessage") || strings.Contains(queryLower, "receivemessage") ||
		strings.Contains(queryLower, "deletequeue") || strings.Contains(queryLower, "createqueue") ||
		strings.Contains(queryLower, "listqueues") || strings.Contains(queryLower, "getqueueattributes") {
		return "sqs"
	}
	if strings.Contains(queryLower, "putitem") || strings.Contains(queryLower, "getitem") ||
		strings.Contains(queryLower, "createtable") || strings.Contains(queryLower, "query") {
		return "dynamodb"
	}
	return ""
}