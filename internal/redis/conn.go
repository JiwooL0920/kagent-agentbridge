package redis

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Conn struct {
	conn   net.Conn
	reader *bufio.Reader
	mu     sync.Mutex
}

type SentinelConfig struct {
	SentinelAddrs []string
	MasterName    string
	DB            int
	DialTimeout   time.Duration
}

func DialSentinel(ctx context.Context, cfg SentinelConfig) (*Conn, error) {
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 5 * time.Second
	}

	var masterAddr string
	var lastErr error

	for _, sentinelAddr := range cfg.SentinelAddrs {
		sentinelConn, err := net.DialTimeout("tcp", sentinelAddr, cfg.DialTimeout)
		if err != nil {
			lastErr = err
			continue
		}

	reader := bufio.NewReader(sentinelConn)
	if _, err := sentinelConn.Write([]byte("*3\r\n$8\r\nSENTINEL\r\n$23\r\nget-master-addr-by-name\r\n$" + 
		strconv.Itoa(len(cfg.MasterName)) + "\r\n" + cfg.MasterName + "\r\n")); err != nil {
			sentinelConn.Close()
			lastErr = err
			continue
		}

		resp, err := parseRESP2(reader)
		sentinelConn.Close()
		if err != nil {
			lastErr = err
			continue
		}

		arr, ok := resp.([]interface{})
		if !ok || len(arr) != 2 {
			lastErr = fmt.Errorf("unexpected sentinel response format")
			continue
		}

		host, ok1 := arr[0].(string)
		port, ok2 := arr[1].(string)
		if !ok1 || !ok2 {
			lastErr = fmt.Errorf("invalid master address format")
			continue
		}

		masterAddr = net.JoinHostPort(host, port)
		break
	}

	if masterAddr == "" {
		return nil, fmt.Errorf("failed to resolve master from sentinel: %w", lastErr)
	}

	masterConn, err := net.DialTimeout("tcp", masterAddr, cfg.DialTimeout)
	if err != nil {
		return nil, fmt.Errorf("dial master %s: %w", masterAddr, err)
	}

	conn := &Conn{
		conn:   masterConn,
		reader: bufio.NewReader(masterConn),
	}

	if cfg.DB != 0 {
		if err := conn.selectDB(cfg.DB); err != nil {
			conn.Close()
			return nil, fmt.Errorf("select DB %d: %w", cfg.DB, err)
		}
	}

	return conn, nil
}

func (c *Conn) Close() error {
	return c.conn.Close()
}

func (c *Conn) selectDB(db int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	cmd := fmt.Sprintf("*2\r\n$6\r\nSELECT\r\n$%d\r\n%d\r\n", len(strconv.Itoa(db)), db)
	if _, err := c.conn.Write([]byte(cmd)); err != nil {
		return err
	}

	resp, err := parseRESP2(c.reader)
	if err != nil {
		return err
	}

	if str, ok := resp.(string); ok && str == "OK" {
		return nil
	}

	return fmt.Errorf("SELECT failed: %v", resp)
}

func (c *Conn) XAdd(stream string, maxlen int, fields map[string]string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	args := []string{"XADD", stream}
	if maxlen > 0 {
		args = append(args, "MAXLEN", "~", strconv.Itoa(maxlen))
	}
	args = append(args, "*")

	for k, v := range fields {
		args = append(args, k, v)
	}

	cmd := buildRESP2Array(args)
	if _, err := c.conn.Write([]byte(cmd)); err != nil {
		return "", err
	}

	resp, err := parseRESP2(c.reader)
	if err != nil {
		return "", err
	}

	id, ok := resp.(string)
	if !ok {
		return "", fmt.Errorf("unexpected XADD response: %v", resp)
	}

	return id, nil
}

func (c *Conn) XReadGroup(group, consumer, stream, id string, count int, blockMS int64) ([]StreamMessage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	args := []string{"XREADGROUP", "GROUP", group, consumer}
	if count > 0 {
		args = append(args, "COUNT", strconv.Itoa(count))
	}
	if blockMS >= 0 {
		args = append(args, "BLOCK", strconv.FormatInt(blockMS, 10))
	}
	args = append(args, "STREAMS", stream, id)

	cmd := buildRESP2Array(args)
	if _, err := c.conn.Write([]byte(cmd)); err != nil {
		return nil, err
	}

	resp, err := parseRESP2(c.reader)
	if err != nil {
		return nil, err
	}

	if resp == nil {
		return nil, nil
	}

	return parseStreamMessages(resp)
}

func (c *Conn) XAck(stream, group, id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	args := []string{"XACK", stream, group, id}
	cmd := buildRESP2Array(args)
	if _, err := c.conn.Write([]byte(cmd)); err != nil {
		return err
	}

	_, err := parseRESP2(c.reader)
	return err
}

func (c *Conn) Get(key string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cmd := buildRESP2Array([]string{"GET", key})
	if _, err := c.conn.Write([]byte(cmd)); err != nil {
		return "", err
	}

	resp, err := parseRESP2(c.reader)
	if err != nil {
		return "", err
	}

	if resp == nil {
		return "", nil
	}

	str, ok := resp.(string)
	if !ok {
		return "", fmt.Errorf("unexpected GET response: %v", resp)
	}

	return str, nil
}

func (c *Conn) Set(key, value string, ttlSeconds int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	args := []string{"SET", key, value}
	if ttlSeconds > 0 {
		args = append(args, "EX", strconv.Itoa(ttlSeconds))
	}

	cmd := buildRESP2Array(args)
	if _, err := c.conn.Write([]byte(cmd)); err != nil {
		return err
	}

	resp, err := parseRESP2(c.reader)
	if err != nil {
		return err
	}

	if str, ok := resp.(string); ok && str == "OK" {
		return nil
	}

	return fmt.Errorf("SET failed: %v", resp)
}

func (c *Conn) XAutoClaim(stream, group, consumer string, minIdleMs int64, start string, count int) ([]StreamMessage, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	args := []string{"XAUTOCLAIM", stream, group, consumer, strconv.FormatInt(minIdleMs, 10), start}
	if count > 0 {
		args = append(args, "COUNT", strconv.Itoa(count))
	}

	cmd := buildRESP2Array(args)
	if _, err := c.conn.Write([]byte(cmd)); err != nil {
		return nil, "", err
	}

	resp, err := parseRESP2(c.reader)
	if err != nil {
		return nil, "", err
	}

	arr, ok := resp.([]interface{})
	if !ok || len(arr) < 2 {
		return nil, "", fmt.Errorf("unexpected XAUTOCLAIM response format")
	}

	nextStart, ok := arr[0].(string)
	if !ok {
		return nil, "", fmt.Errorf("invalid next start cursor")
	}

	messages, err := parseStreamMessages(arr[1])
	return messages, nextStart, err
}

func (c *Conn) HGetAll(key string) (map[string]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cmd := buildRESP2Array([]string{"HGETALL", key})
	if _, err := c.conn.Write([]byte(cmd)); err != nil {
		return nil, err
	}

	resp, err := parseRESP2(c.reader)
	if err != nil {
		return nil, err
	}

	arr, ok := resp.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected HGETALL response: %v", resp)
	}

	result := make(map[string]string)
	for i := 0; i < len(arr); i += 2 {
		if i+1 >= len(arr) {
			break
		}
		k, ok1 := arr[i].(string)
		v, ok2 := arr[i+1].(string)
		if ok1 && ok2 {
			result[k] = v
		}
	}

	return result, nil
}

type StreamMessage struct {
	ID     string
	Fields map[string]string
}

func buildRESP2Array(args []string) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("*%d\r\n", len(args)))
	for _, arg := range args {
		sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))
	}
	return sb.String()
}

func parseRESP2(r *bufio.Reader) (interface{}, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}

	line = strings.TrimSuffix(line, "\r\n")
	if len(line) == 0 {
		return nil, fmt.Errorf("empty response")
	}

	switch line[0] {
	case '+':
		return line[1:], nil
	case '-':
		return nil, fmt.Errorf("redis error: %s", line[1:])
	case ':':
		n, err := strconv.ParseInt(line[1:], 10, 64)
		if err != nil {
			return nil, err
		}
		return n, nil
	case '$':
		length, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, err
		}
		if length == -1 {
			return nil, nil
		}
		buf := make([]byte, length+2)
		if _, err := r.Read(buf); err != nil {
			return nil, err
		}
		return string(buf[:length]), nil
	case '*':
		count, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, err
		}
		if count == -1 {
			return nil, nil
		}
		arr := make([]interface{}, count)
		for i := 0; i < count; i++ {
			elem, err := parseRESP2(r)
			if err != nil {
				return nil, err
			}
			arr[i] = elem
		}
		return arr, nil
	default:
		return nil, fmt.Errorf("unknown RESP2 type: %c", line[0])
	}
}

func parseStreamMessages(data interface{}) ([]StreamMessage, error) {
	streamArr, ok := data.([]interface{})
	if !ok {
		return nil, nil
	}

	if len(streamArr) == 0 {
		return nil, nil
	}

	if len(streamArr) == 1 {
		streamData, ok := streamArr[0].([]interface{})
		if !ok || len(streamData) < 2 {
			return nil, nil
		}
		messagesData, ok := streamData[1].([]interface{})
		if !ok {
			return nil, nil
		}
		streamArr = messagesData
	}

	var messages []StreamMessage
	for _, item := range streamArr {
		msgArr, ok := item.([]interface{})
		if !ok || len(msgArr) != 2 {
			continue
		}

		id, ok1 := msgArr[0].(string)
		fieldsArr, ok2 := msgArr[1].([]interface{})
		if !ok1 || !ok2 {
			continue
		}

		fields := make(map[string]string)
		for i := 0; i < len(fieldsArr); i += 2 {
			if i+1 >= len(fieldsArr) {
				break
			}
			k, ok1 := fieldsArr[i].(string)
			v, ok2 := fieldsArr[i+1].(string)
			if ok1 && ok2 {
				fields[k] = v
			}
		}

		messages = append(messages, StreamMessage{
			ID:     id,
			Fields: fields,
		})
	}

	return messages, nil
}
