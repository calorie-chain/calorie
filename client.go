
package queue


var gid int64

type Client interface {
	Send(msg *Message, waitReply bool) (err error) 
	SendTimeout(msg *Message, waitReply bool, timeout time.Duration) (err error)
	Wait(msg *Message) (*Message, error)                              
	WaitTimeout(msg *Message, timeout time.Duration) (*Message, error) 	Recv() chan *Message
	Reply(msg *Message)
	Sub(topic string) 
	Close()
	CloseQueue() (*types.Reply, error)
	NewMessage(topic string, ty int64, data interface{}) (msg *Message)
	GetConfig() *types.CalorieConfig
}

type Module interface {
	SetQueueClient(client Client)
	Wait()
	Close()
}

type client struct {
	q          *queue
	recv       chan *Message
	done       chan struct{}
	wg         *sync.WaitGroup
	topic      unsafe.Pointer
	isClosed   int32
	isCloseing int32
}

func newClient(q *queue) Client {
	client := &client{}
	client.q = q
	client.recv = make(chan *Message, 5)
	client.done = make(chan struct{}, 1)
	client.wg = &sync.WaitGroup{}
	return client
}

func (client *client) GetConfig() *types.CalorieConfig {
	types.AssertConfig(client.q)
	cfg := client.q.GetConfig()
	if cfg == nil {
		panic("CalorieConfig is nil")
	}
	return cfg
}

func (client *client) Send(msg *Message, waitReply bool) (err error) {
	timeout := time.Duration(-1)
	err = client.SendTimeout(msg, waitReply, timeout)
	if err == ErrQueueTimeout {
		panic(err)
	}
	return err
}

func (client *client) SendTimeout(msg *Message, waitReply bool, timeout time.Duration) (err error) {
	if client.isClose() {
		return ErrIsQueueClosed
	}
	if !waitReply {
		msg.chReply = nil
		return client.q.sendLowTimeout(msg, timeout)
	}
	return client.q.send(msg, timeout)
}


func (client *client) NewMessage(topic string, ty int64, data interface{}) (msg *Message) {
	id := atomic.AddInt64(&gid, 1)
	return NewMessage(id, topic, ty, data)
}

func (client *client) Reply(msg *Message) {
	if msg.chReply != nil {
		msg.Reply(msg)
		return
	}
	if msg.callback != nil {
		client.q.callback <- msg
	}
}

func (client *client) WaitTimeout(msg *Message, timeout time.Duration) (*Message, error) {
	if msg.chReply == nil {
		return &Message{}, errors.New("empty wait channel")
	}

	var t <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		t = timer.C
	}
	select {
	case msg = <-msg.chReply:
		return msg, msg.Err()
	case <-client.done:
		return &Message{}, ErrIsQueueClosed
	case <-t:
		return &Message{}, ErrQueueTimeout
	}
}

func (client *client) Wait(msg *Message) (*Message, error) {
	timeout := time.Duration(-1)
	msg, err := client.WaitTimeout(msg, timeout)
	if err == ErrQueueTimeout {
		panic(err)
	}
	return msg, err
}

func (client *client) Recv() chan *Message {
	return client.recv
}

func (client *client) getTopic() string {
	return *(*string)(atomic.LoadPointer(&client.topic))
}

func (client *client) setTopic(topic string) {
	atomic.StorePointer(&client.topic, unsafe.Pointer(&topic))
}

func (client *client) isClose() bool {
	return atomic.LoadInt32(&client.isClosed) == 1
}

func (client *client) isInClose() bool {
	return atomic.LoadInt32(&client.isCloseing) == 1
}

func (client *client) Close() {
	if atomic.LoadInt32(&client.isClosed) == 1 || atomic.LoadPointer(&client.topic) == nil {
		return
	}
	topic := client.getTopic()
	client.q.closeTopic(topic)
	close(client.done)
	atomic.StoreInt32(&client.isCloseing, 1)
	client.wg.Wait()
	atomic.StoreInt32(&client.isClosed, 1)
	close(client.Recv())
	for msg := range client.Recv() {
		msg.Reply(client.NewMessage(msg.Topic, msg.Ty, types.ErrChannelClosed))
	}
}

func (client *client) CloseQueue() (*types.Reply, error) {
	if client.q.isClosed() {
		return &types.Reply{IsOk: true}, nil
	}
	qlog.Debug("queue", "msg", "closing Calorie")
	client.q.interrupt <- struct{}{}
	return &types.Reply{IsOk: true}, nil
}

func (client *client) isEnd(data *Message, ok bool) bool {
	if !ok {
		return true
	}
	if data.Data == nil && data.ID == 0 && data.Ty == 0 {
		return true
	}
	if atomic.LoadInt32(&client.isClosed) == 1 {
		return true
	}
	return false
}

func (client *client) Sub(topic string) {
	if client.isInClose() || client.isClose() {
		return
	}
	client.wg.Add(1)
	client.setTopic(topic)
	sub := client.q.chanSub(topic)
	go func() {
		defer func() {
			client.wg.Done()
		}()
		for {
			select {
			case data, ok := <-sub.high:
				if client.isEnd(data, ok) {
					qlog.Info("unsub1", "topic", topic)
					return
				}
				client.Recv() <- data
			default:
				select {
				case data, ok := <-sub.high:
					if client.isEnd(data, ok) {
						qlog.Info("unsub2", "topic", topic)
						return
					}
					client.Recv() <- data
				case data, ok := <-sub.low:
					if client.isEnd(data, ok) {
						qlog.Info("unsub3", "topic", topic)
						return
					}
					client.Recv() <- data
				case <-client.done:
					qlog.Error("unsub4", "topic", topic)
					return
				}
			}
		}
	}()
}
