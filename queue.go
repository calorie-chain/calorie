
package queue


var qlog = log.New("module", "queue")

const (
	defaultChanBuffer    = 64
	defaultLowChanBuffer = 40960
)

var (
	ErrIsQueueClosed    = errors.New("ErrIsQueueClosed")
	ErrQueueTimeout     = errors.New("ErrQueueTimeout")
	ErrQueueChannelFull = errors.New("ErrQueueChannelFull")
)

func DisableLog() {
	qlog.SetHandler(log.DiscardHandler())
}

type chanSub struct {
	high    chan *Message
	low     chan *Message
	isClose int32
}

type Queue interface {
	Close()
	Start()
	Client() Client
	Name() string
	SetConfig(cfg *types.CalorieConfig)
	GetConfig() *types.CalorieConfig
}

type queue struct {
	chanSubs  map[string]*chanSub
	mu        sync.Mutex
	done      chan struct{}
	interrupt chan struct{}
	callback  chan *Message
	isClose   int32
	name      string
	cfg       *types.CalorieConfig
}

func New(name string) Queue {
	q := &queue{
		chanSubs:  make(map[string]*chanSub),
		name:      name,
		done:      make(chan struct{}, 1),
		interrupt: make(chan struct{}, 1),
		callback:  make(chan *Message, 1024),
	}
	go func() {
		for {
			select {
			case <-q.done:
				qlog.Info("closing Calorie callback")
				return
			case msg := <-q.callback:
				if msg.callback != nil {
					msg.callback(msg)
				}
			}
		}
	}()
	return q
}

func (q *queue) GetConfig() *types.CalorieConfig {
	return q.cfg
}

func (q *queue) SetConfig(cfg *types.CalorieConfig) {
	if cfg == nil {
		panic("set config is nil")
	}
	if q.cfg != nil {
		panic("do not reset queue config")
	}
	q.cfg = cfg
}

func (q *queue) Name() string {
	return q.name
}

func (q *queue) Start() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	select {
	case <-q.done:
		qlog.Info("closing Calorie done")
		break
	case <-q.interrupt:
		qlog.Info("closing Calorie")
		break
	case s := <-c:
		qlog.Info("Got signal:", s)
		break
	}
}

func (q *queue) isClosed() bool {
	return atomic.LoadInt32(&q.isClose) == 1
}

func (q *queue) Close() {
	if q.isClosed() {
		return
	}
	q.mu.Lock()
	for topic, ch := range q.chanSubs {
		if ch.isClose == 0 {
			ch.high <- &Message{}
			ch.low <- &Message{}
			q.chanSubs[topic] = &chanSub{isClose: 1}
		}
	}
	q.mu.Unlock()
	q.done <- struct{}{}
	close(q.done)
	atomic.StoreInt32(&q.isClose, 1)
	qlog.Info("queue module closed")
}

func (q *queue) chanSub(topic string) *chanSub {
	q.mu.Lock()
	defer q.mu.Unlock()
	_, ok := q.chanSubs[topic]
	if !ok {
		q.chanSubs[topic] = &chanSub{
			high:    make(chan *Message, defaultChanBuffer),
			low:     make(chan *Message, defaultLowChanBuffer),
			isClose: 0,
		}
	}
	return q.chanSubs[topic]
}

func (q *queue) closeTopic(topic string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	sub, ok := q.chanSubs[topic]
	if !ok {
		return
	}
	if sub.isClose == 0 {
		sub.high <- &Message{}
		sub.low <- &Message{}
	}
	q.chanSubs[topic] = &chanSub{isClose: 1}
}

func (q *queue) send(msg *Message, timeout time.Duration) (err error) {
	if q.isClosed() {
		return types.ErrChannelClosed
	}
	sub := q.chanSub(msg.Topic)
	if sub.isClose == 1 {
		return types.ErrChannelClosed
	}
	if timeout == -1 {
		sub.high <- msg
		return nil
	}
	defer func() {
		res := recover()
		if res != nil {
			err = res.(error)
		}
	}()
	if timeout == 0 {
		select {
		case sub.high <- msg:
			return nil
		default:
			qlog.Error("send chainfull", "msg", msg, "topic", msg.Topic, "sub", sub)
			return ErrQueueChannelFull
		}
	}
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case sub.high <- msg:
	case <-t.C:
		qlog.Error("send timeout", "msg", msg, "topic", msg.Topic, "sub", sub)
		return ErrQueueTimeout
	}
	return nil
}

func (q *queue) sendAsyn(msg *Message) error {
	if q.isClosed() {
		return types.ErrChannelClosed
	}
	sub := q.chanSub(msg.Topic)
	if sub.isClose == 1 {
		return types.ErrChannelClosed
	}
	select {
	case sub.low <- msg:
		return nil
	default:
		qlog.Error("send asyn err", "msg", msg, "err", ErrQueueChannelFull)
		return ErrQueueChannelFull
	}
}

func (q *queue) sendLowTimeout(msg *Message, timeout time.Duration) error {
	if q.isClosed() {
		return types.ErrChannelClosed
	}
	sub := q.chanSub(msg.Topic)
	if sub.isClose == 1 {
		return types.ErrChannelClosed
	}
	if timeout == -1 {
		sub.low <- msg
		return nil
	}
	if timeout == 0 {
		return q.sendAsyn(msg)
	}
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case sub.low <- msg:
		return nil
	case <-t.C:
		qlog.Error("send asyn timeout", "msg", msg)
		return ErrQueueTimeout
	}
}

func (q *queue) Client() Client {
	return newClient(q)
}

type Message struct {
	Topic    string
	Ty       int64
	ID       int64
	Data     interface{}
	chReply  chan *Message
	callback func(msg *Message)
}

func NewMessage(id int64, topic string, ty int64, data interface{}) (msg *Message) {
	msg = &Message{}
	msg.ID = id
	msg.Ty = ty
	msg.Data = data
	msg.Topic = topic
	msg.chReply = make(chan *Message, 1)
	return msg
}

func NewMessageCallback(id int64, topic string, ty int64, data interface{}, callback func(msg *Message)) (msg *Message) {
	msg = &Message{}
	msg.ID = id
	msg.Ty = ty
	msg.Data = data
	msg.Topic = topic
	msg.callback = callback
	return msg
}

func (msg *Message) GetData() interface{} {
	if _, ok := msg.Data.(error); ok {
		return nil
	}
	return msg.Data
}

func (msg *Message) Err() error {
	if err, ok := msg.Data.(error); ok {
		return err
	}
	return nil
}

func (msg *Message) Reply(replyMsg *Message) {
	if msg.chReply == nil {
		qlog.Debug("reply a empty chreply", "msg", msg)
		return
	}
	msg.chReply <- replyMsg
	if msg.Topic != "store" {
		qlog.Debug("reply msg ok", "msg", msg)
	}
}

func (msg *Message) String() string {
	return fmt.Sprintf("{topic:%s, Ty:%s, Id:%d, Err:%v, Ch:%v}", msg.Topic,
		types.GetEventName(int(msg.Ty)), msg.ID, msg.Err(), msg.chReply != nil)
}

func (msg *Message) ReplyErr(title string, err error) {
	var reply types.Reply
	if err != nil {
		qlog.Error(title, "reply.err", err.Error())
		reply.IsOk = false
		reply.Msg = []byte(err.Error())
	} else {
		qlog.Debug(title, "success", "ok")
		reply.IsOk = true
	}
	id := atomic.AddInt64(&gid, 1)
	msg.Reply(NewMessage(id, "", types.EventReply, &reply))
}
