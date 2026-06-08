package redis

import (
	"bufio"
	"context"
	"log/slog"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

func TestBuildRESP2Array(t *testing.T) {
	result := buildRESP2Array([]string{"SET", "key", "value"})
	expected := "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
	if result != expected {
		t.Errorf("expected %q, got %q", expected, result)
	}
}

func TestParseRESP2_SimpleString(t *testing.T) {
	input := "+OK\r\n"
	reader := bufio.NewReader(strings.NewReader(input))
	result, err := parseRESP2(reader)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "OK" {
		t.Errorf("expected OK, got %v", result)
	}
}

func TestParseRESP2_BulkString(t *testing.T) {
	input := "$5\r\nhello\r\n"
	reader := bufio.NewReader(strings.NewReader(input))
	result, err := parseRESP2(reader)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "hello" {
		t.Errorf("expected hello, got %v", result)
	}
}

func TestParseRESP2_Integer(t *testing.T) {
	input := ":42\r\n"
	reader := bufio.NewReader(strings.NewReader(input))
	result, err := parseRESP2(reader)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != int64(42) {
		t.Errorf("expected 42, got %v", result)
	}
}

func TestParseRESP2_Array(t *testing.T) {
	input := "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
	reader := bufio.NewReader(strings.NewReader(input))
	result, err := parseRESP2(reader)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	arr, ok := result.([]interface{})
	if !ok {
		t.Fatalf("expected array, got %T", result)
	}
	if len(arr) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(arr))
	}
	if arr[0] != "foo" || arr[1] != "bar" {
		t.Errorf("expected [foo bar], got %v", arr)
	}
}

func TestParseRESP2_NullBulkString(t *testing.T) {
	input := "$-1\r\n"
	reader := bufio.NewReader(strings.NewReader(input))
	result, err := parseRESP2(reader)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestParseRESP2_Error(t *testing.T) {
	input := "-ERR unknown command\r\n"
	reader := bufio.NewReader(strings.NewReader(input))
	_, err := parseRESP2(reader)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "unknown command") {
		t.Errorf("expected error message to contain 'unknown command', got: %v", err)
	}
}

func TestStreamPublisher(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start test server: %v", err)
	}
	defer listener.Close()

	serverAddr := listener.Addr().String()

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		reader := bufio.NewReader(conn)
		for {
			cmd, err := parseRESP2(reader)
			if err != nil {
				return
			}

			cmdArr, ok := cmd.([]interface{})
			if !ok || len(cmdArr) == 0 {
				continue
			}

			operation, ok := cmdArr[0].(string)
			if !ok {
				continue
			}

			switch operation {
			case "SET":
				conn.Write([]byte("+OK\r\n"))
			case "XADD":
				conn.Write([]byte("$15\r\n1234567890-0\r\n"))
			default:
				conn.Write([]byte("-ERR unknown command\r\n"))
			}
		}
	}()

	time.Sleep(100 * time.Millisecond)

	netConn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		t.Fatalf("failed to connect to test server: %v", err)
	}

	conn := &Conn{
		conn:   netConn,
		reader: bufio.NewReader(netConn),
	}
	defer conn.Close()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	publisher := NewStreamPublisher(conn, "test-stream", logger)

	id, _, state, err := publisher.SendTask(context.Background(), "test-agent", "req-123", "test task")
	if err != nil {
		t.Fatalf("SendTask failed: %v", err)
	}

	if id == "" {
		t.Error("expected non-empty task ID")
	}
	if state != "published" {
		t.Errorf("expected state 'published', got %s", state)
	}
}

func TestParseStreamMessages(t *testing.T) {
	data := []interface{}{
		[]interface{}{
			"stream-name",
			[]interface{}{
				[]interface{}{
					"1234567890-0",
					[]interface{}{"field1", "value1", "field2", "value2"},
				},
			},
		},
	}

	messages, err := parseStreamMessages(data)
	if err != nil {
		t.Fatalf("parseStreamMessages failed: %v", err)
	}

	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}

	msg := messages[0]
	if msg.ID != "1234567890-0" {
		t.Errorf("expected ID 1234567890-0, got %s", msg.ID)
	}
	if len(msg.Fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(msg.Fields))
	}
	if msg.Fields["field1"] != "value1" {
		t.Errorf("expected field1=value1, got %s", msg.Fields["field1"])
	}
}
