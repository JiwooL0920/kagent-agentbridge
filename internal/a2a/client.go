package a2a

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

func (c *Client) SendTask(ctx context.Context, agent, requestID, text string) (string, string, string, error) {
	reqBody := TaskSendRequest{
		JSONRPC: "2.0",
		ID:      requestID,
		Method:  "message/send",
		Params: TaskSendParams{
			Message: MessageBody{
				MessageID: requestID,
				Role:      "user",
				Parts: []MessagePart{
					{Kind: "text", Text: text},
				},
			},
			Configuration: TaskSendConfiguration{
				Blocking:      false,
				HistoryLength: 0,
			},
		},
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return "", "", "", fmt.Errorf("marshal request: %w", err)
	}

	url := fmt.Sprintf("%s/agents/%s/", c.baseURL, agent)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(jsonData))
	if err != nil {
		return "", "", "", fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if c.bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.bearerToken)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", "", "", fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", "", "", fmt.Errorf("non-200 status: %d, body: %s", resp.StatusCode, string(body))
	}

	var taskResp TaskSendResponse
	if err := json.NewDecoder(resp.Body).Decode(&taskResp); err != nil {
		return "", "", "", fmt.Errorf("decode response: %w", err)
	}

	if taskResp.Error != nil {
		return "", "", "", fmt.Errorf("A2A error: %d - %s", taskResp.Error.Code, taskResp.Error.Message)
	}

	if taskResp.Result == nil {
		return "", "", "", fmt.Errorf("no result in response")
	}

	c.logger.Info("task sent to agent",
		"agent", agent,
		"taskID", taskResp.Result.TaskID,
		"contextID", taskResp.Result.ContextID,
		"state", taskResp.Result.State)

	return taskResp.Result.TaskID, taskResp.Result.ContextID, taskResp.Result.State, nil
}
