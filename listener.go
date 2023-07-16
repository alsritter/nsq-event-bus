package bus

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	nsq "github.com/nsqio/go-nsq"
)

var (
	// ErrTopicRequired is returned when topic is not passed as parameter.
	ErrTopicRequired = errors.New("topic is mandatory")
	// ErrHandlerRequired is returned when handler is not passed as parameter.
	ErrHandlerRequired = errors.New("handler is mandatory")
	// ErrChannelRequired is returned when channel is not passed as parameter in bus.ListenerConfig.
	ErrChannelRequired = errors.New("channel is mandatory")
)

// HandlerFunc is the handler function to handle the massage.
type HandlerFunc func(m *Message) (interface{}, error)

// On listen to a message from a specific topic using nsq consumer, returns
// an error if topic and channel not passed or if an error occurred while creating
// nsq consumer.
func On(lc ListenerConfig) error {
	if len(lc.Topic) == 0 {
		return ErrTopicRequired
	}

	if len(lc.Channel) == 0 {
		return ErrChannelRequired
	}

	if lc.HandlerFunc == nil {
		return ErrHandlerRequired
	}

	if len(lc.Lookup) == 0 {
		lc.Lookup = []string{"localhost:4161"}
	}

	if lc.HandlerConcurrency == 0 {
		lc.HandlerConcurrency = 1
	}

	// create topic if not exists
	if lc.AutoCreateTopic {
		exist, err := checkTopic(lc.Lookup[0], lc.Topic)
		if err != nil {
			return err
		}

		if !exist {
			if err := createTopic(lc.Topic, lc.Lookup[0]); err != nil {
				return err
			}
		}
	}

	config := newListenerConfig(lc)
	consumer, err := nsq.NewConsumer(lc.Topic, lc.Channel, config)
	if err != nil {
		return err
	}

	handler := handleMessage(lc)
	consumer.AddConcurrentHandlers(handler, lc.HandlerConcurrency)
	return consumer.ConnectToNSQLookupds(lc.Lookup)
}

func handleMessage(lc ListenerConfig) nsq.HandlerFunc {
	return nsq.HandlerFunc(func(message *nsq.Message) error {
		m := Message{Message: message}
		if err := json.Unmarshal(message.Body, &m); err != nil {
			return err
		}

		res, err := lc.HandlerFunc(&m)
		if err != nil {
			return err
		}

		if m.ReplyTo == "" {
			return nil
		}

		emitter, err := NewEmitter(EmitterConfig{})
		if err != nil {
			return err
		}

		return emitter.Emit(m.ReplyTo, res)
	})
}

func checkTopic(lookupAddress string, topic string) (bool, error) {
	url := fmt.Sprintf("http://%s/lookup?topic=%s", lookupAddress, topic)
	resp, err := http.Get(url)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		// topic 存在
		return true, nil
	} else if resp.StatusCode == http.StatusNotFound {
		// topic 不存在
		return false, nil
	} else {
		// 其他状态码，处理错误
		return false, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
}

func createTopic(topic string, lookupAddress string) error {
	lookupURL := fmt.Sprintf("http://%s/nodes", lookupAddress)
	resp, err := http.Get(lookupURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to get nsqd address from lookup: %s", resp.Status)
	}

	var nodes []struct {
		Address string `json:"broadcast_address"`
		TCPPort int    `json:"tcp_port"`
	}

	err = json.NewDecoder(resp.Body).Decode(&nodes)
	if err != nil {
		return err
	}

	if len(nodes) == 0 {
		return errors.New("no nsqd nodes found in lookup")
	}

	nsqdAddress := fmt.Sprintf("%s:%d", nodes[0].Address, nodes[0].TCPPort)
	createURL := fmt.Sprintf("http://%s/topic/create?topic=%s", nsqdAddress, topic)
	createResp, err := http.Post(createURL, "", nil)
	if err != nil {
		return err
	}
	defer createResp.Body.Close()

	if createResp.StatusCode == http.StatusOK {
		// topic 创建成功
		return nil
	} else {
		// 处理错误
		return fmt.Errorf("failed to create topic: %s", createResp.Status)
	}
}
