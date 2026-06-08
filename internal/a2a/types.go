package a2a

import (
	"log/slog"
	"net/http"
)

// Client handles A2A JSON-RPC communication with kagent coordinator-agent.
type Client struct {
	baseURL     string
	httpClient  *http.Client
	bearerToken string
	logger      *slog.Logger
}

// NewClient creates a new A2A client.
func NewClient(baseURL, bearerToken string, logger *slog.Logger) *Client {
	return &Client{
		baseURL:     baseURL,
		httpClient:  &http.Client{},
		bearerToken: bearerToken,
		logger:      logger,
	}
}

// TaskSendRequest represents the JSON-RPC request to send a task.
type TaskSendRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      string          `json:"id"`
	Method  string          `json:"method"`
	Params  TaskSendParams  `json:"params"`
}

// TaskSendParams contains the message and configuration for task sending.
type TaskSendParams struct {
	Message       MessageBody           `json:"message"`
	Configuration TaskSendConfiguration `json:"configuration"`
}

// MessageBody represents the A2A message structure.
type MessageBody struct {
	MessageID string        `json:"messageId"`
	Role      string        `json:"role"`
	Parts     []MessagePart `json:"parts"`
}

// MessagePart represents a single part of the message (text, data, etc).
type MessagePart struct {
	Kind string `json:"kind"`
	Text string `json:"text,omitempty"`
}

// TaskSendConfiguration contains configuration for task sending.
type TaskSendConfiguration struct {
	Blocking      bool `json:"blocking"`
	HistoryLength int  `json:"historyLength"`
}

// TaskSendResponse represents the JSON-RPC response from the A2A server.
type TaskSendResponse struct {
	JSONRPC string               `json:"jsonrpc"`
	ID      string               `json:"id"`
	Result  *TaskSendResult      `json:"result,omitempty"`
	Error   *TaskSendErrorDetail `json:"error,omitempty"`
}

// TaskSendResult contains the task state from the response.
type TaskSendResult struct {
	TaskID    string `json:"taskId"`
	ContextID string `json:"contextId"`
	State     string `json:"state"`
}

// TaskSendErrorDetail represents an error in the JSON-RPC response.
type TaskSendErrorDetail struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}
