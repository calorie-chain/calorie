package p2p

var (
	log = l.New("module", "p2p")
)

type P2p struct {
	api          client.QueueProtocolAPI
	client       queue.Client
	node         *Node
	p2pCli       EventInterface
	txCapcity    int32
	txFactory    chan struct{}
	otherFactory chan struct{}
	waitRestart  chan struct{}
	taskGroup    *sync.WaitGroup

	closed  int32
	restart int32
	cfg     *types.P2P
}

func New(cfg *types.CalorieConfig) *P2p {
	mcfg := cfg.GetModuleConfig().P2P
	if cfg.IsTestNet() && mcfg.Channel == 0 {
		mcfg.Channel = defaultTestNetChannel
	}
	if mcfg.LightTxTTL <= 1 {
		mcfg.LightTxTTL = DefaultLtTxBroadCastTTL
	}
	if mcfg.MaxTTL <= 0 {
		mcfg.MaxTTL = DefaultMaxTxBroadCastTTL
	}

	if mcfg.MinLtBlockTxNum < DefaultMinLtBlockTxNum {
		mcfg.MinLtBlockTxNum = DefaultMinLtBlockTxNum
	}

	log.Info("p2p", "Channel", mcfg.Channel, "Version", VERSION, "IsTest", cfg.IsTestNet())
	if mcfg.InnerBounds == 0 {
		mcfg.InnerBounds = 500
	}
	log.Info("p2p", "InnerBounds", mcfg.InnerBounds)

	node, err := NewNode(cfg)
	if err != nil {
		log.Error(err.Error())
		return nil
	}
	p2p := new(P2p)
	p2p.node = node
	p2p.p2pCli = NewP2PCli(p2p)
	p2p.txFactory = make(chan struct{}, 1000)    
	p2p.otherFactory = make(chan struct{}, 1000) 0
	p2p.waitRestart = make(chan struct{}, 1)
	p2p.txCapcity = 1000
	p2p.cfg = mcfg
	p2p.taskGroup = &sync.WaitGroup{}
	return p2p
}

func (network *P2p) Wait() {}

func (network *P2p) isClose() bool {
	return atomic.LoadInt32(&network.closed) == 1
}

func (network *P2p) isRestart() bool {
	return atomic.LoadInt32(&network.restart) == 1
}

func (network *P2p) Close() {
	log.Info("p2p network start shutdown")
	atomic.StoreInt32(&network.closed, 1)
	network.waitTaskDone()
	network.node.Close()
	if network.client != nil {
		network.client.Close()
	}
}

func (network *P2p) SetQueueClient(cli queue.Client) {
	var err error
	if network.client == nil {
		network.client = cli

	}
	network.api, err = client.New(cli, nil)
	if err != nil {
		panic("SetQueueClient client.New err")
	}
	network.node.SetQueueClient(cli)

	go func(p2p *P2p) {

		if p2p.isRestart() {
			p2p.node.Start()
			atomic.StoreInt32(&p2p.restart, 0)
			network.waitRestart <- struct{}{}
			return
		}

		p2p.subP2pMsg()
		key, pub := p2p.node.nodeInfo.addrBook.GetPrivPubKey()
		log.Debug("key pub:", pub, "")
		if key == "" {
			if p2p.cfg.WaitPid { 
				if p2p.genAirDropKeyFromWallet() != nil {
					return
				}
			} else {
				p2p.node.nodeInfo.addrBook.ResetPeerkey(key, pub)
				go p2p.genAirDropKeyFromWallet()
			}

		} else {
			go p2p.genAirDropKeyFromWallet()

		}
		p2p.node.Start()
		log.Debug("SetQueueClient gorountine ret")

	}(network)
}

func (network *P2p) loadP2PPrivKeyToWallet() error {
	var parm types.ReqWalletImportPrivkey
	parm.Privkey, _ = network.node.nodeInfo.addrBook.GetPrivPubKey()
	parm.Label = "node award"

ReTry:
	resp, err := network.api.ExecWalletFunc("wallet", "WalletImportPrivkey", &parm)
	if err != nil {
		if err == types.ErrPrivkeyExist {
			return nil
		}
		if err == types.ErrLabelHasUsed {
			parm.Label = fmt.Sprintf("node award %v", P2pComm.RandStr(3))
			time.Sleep(time.Second)
			goto ReTry
		}
		log.Error("loadP2PPrivKeyToWallet", "err", err.Error())
		return err
	}

	log.Debug("loadP2PPrivKeyToWallet", "resp", resp.(*types.WalletAccount))
	return nil
}

func (network *P2p) showTaskCapcity() {
	ticker := time.NewTicker(time.Second * 5)
	log.Info("ShowTaskCapcity", "Capcity", atomic.LoadInt32(&network.txCapcity))
	defer ticker.Stop()
	for {
		if network.isClose() {
			log.Debug("ShowTaskCapcity", "loop", "done")
			return
		}
		<-ticker.C
		log.Debug("ShowTaskCapcity", "Capcity", atomic.LoadInt32(&network.txCapcity))
	}
}

func (network *P2p) genAirDropKeyFromWallet() error {
	_, savePub := network.node.nodeInfo.addrBook.GetPrivPubKey()
	for {
		if network.isClose() {
			log.Error("genAirDropKeyFromWallet", "p2p closed", "")
			return fmt.Errorf("p2p closed")
		}

		resp, err := network.api.ExecWalletFunc("wallet", "GetWalletStatus", &types.ReqNil{})
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		if resp.(*types.WalletStatus).GetIsWalletLock() { 
			if savePub == "" {
				log.Warn("P2P Stuck ! Wallet must be unlock and save with mnemonics")

			}
			time.Sleep(time.Second)
			continue
		}

		if !resp.(*types.WalletStatus).GetIsHasSeed() { 
			if savePub == "" {
				log.Warn("P2P Stuck ! Wallet must be imported with mnemonics")

			}
			time.Sleep(time.Second * 5)
			continue
		}

		break
	}

	r := rand.New(rand.NewSource(types.Now().Unix()))
	var minIndex int32 = 100000000
	randIndex := minIndex + r.Int31n(1000000)
	reqIndex := &types.Int32{Data: randIndex}
	msg, err := network.api.ExecWalletFunc("wallet", "NewAccountByIndex", reqIndex)
	if err != nil {
		log.Error("genAirDropKeyFromWallet", "err", err)
		return err
	}
	var hexPrivkey string
	if reply, ok := msg.(*types.ReplyString); !ok {
		log.Error("genAirDropKeyFromWallet", "wrong format data", "")
		panic(err)

	} else {
		hexPrivkey = reply.GetData()
	}
	if hexPrivkey[:2] == "0x" {
		hexPrivkey = hexPrivkey[2:]
	}

	hexPubkey, err := P2pComm.Pubkey(hexPrivkey)
	if err != nil {
		log.Error("genAirDropKeyFromWallet", "gen pub error", err)
		panic(err)
	}

	log.Info("genAirDropKeyFromWallet", "pubkey", hexPubkey)

	if savePub == hexPubkey {
		return nil
	}

	if savePub != "" {
		err = network.loadP2PPrivKeyToWallet()
		if err != nil {
			log.Error("genAirDropKeyFromWallet", "loadP2PPrivKeyToWallet error", err)
			panic(err)
		}
		network.node.nodeInfo.addrBook.ResetPeerkey(hexPrivkey, hexPubkey)
		log.Info("genAirDropKeyFromWallet", "p2p will Restart....")
		network.ReStart()
		return nil
	}
	network.node.nodeInfo.addrBook.ResetPeerkey(hexPrivkey, hexPubkey)

	return nil
}

func (network *P2p) ReStart() {
	if !atomic.CompareAndSwapInt32(&network.restart, 0, 1) {
		return
	}
	log.Info("p2p restart, wait p2p task done")
	network.waitTaskDone()
	network.node.Close()
	types.AssertConfig(network.client)
	node, err := NewNode(network.client.GetConfig()) 
	if err != nil {
		panic(err.Error())
	}
	network.node = node
	network.SetQueueClient(network.client)

}

func (network *P2p) subP2pMsg() {
	if network.client == nil {
		return
	}

	go network.showTaskCapcity()
	go func() {

		var taskIndex int64
		network.client.Sub("p2p")
		for msg := range network.client.Recv() {

			if network.isClose() {
				log.Debug("subP2pMsg", "loop", "done")
				close(network.otherFactory)
				close(network.txFactory)
				return
			}
			taskIndex++
			log.Debug("p2p recv", "msg", types.GetEventName(int(msg.Ty)), "msg type", msg.Ty, "taskIndex", taskIndex)
			if msg.Ty == types.EventTxBroadcast {
				network.txFactory <- struct{}{} 
				atomic.AddInt32(&network.txCapcity, -1)
			} else {
				if msg.Ty != types.EventPeerInfo {
					network.otherFactory <- struct{}{}
				}
			}
			switch msg.Ty {

			case types.EventTxBroadcast: 
				network.processEvent(msg, taskIndex, network.p2pCli.BroadCastTx)
			case types.EventBlockBroadcast: 
				network.processEvent(msg, taskIndex, network.p2pCli.BlockBroadcast)
			case types.EventFetchBlocks:
				network.processEvent(msg, taskIndex, network.p2pCli.GetBlocks)
			case types.EventGetMempool:
				network.processEvent(msg, taskIndex, network.p2pCli.GetMemPool)
			case types.EventPeerInfo:
				network.processEvent(msg, taskIndex, network.p2pCli.GetPeerInfo)
			case types.EventFetchBlockHeaders:
				network.processEvent(msg, taskIndex, network.p2pCli.GetHeaders)
			case types.EventGetNetInfo:
				network.processEvent(msg, taskIndex, network.p2pCli.GetNetInfo)
			default:
				log.Warn("unknown msgtype", "msg", msg)
				msg.Reply(network.client.NewMessage("", msg.Ty, types.Reply{Msg: []byte("unknown msgtype")}))
				<-network.otherFactory
				continue
			}
		}
		log.Info("subP2pMsg", "loop", "close")

	}()

}

func (network *P2p) processEvent(msg *queue.Message, taskIdx int64, eventFunc p2pEventFunc) {

	if network.isRestart() {
		log.Info("wait for p2p restart....")
		<-network.waitRestart
		log.Info("p2p restart ok....")
	}
	network.taskGroup.Add(1)
	go func() {
		defer network.taskGroup.Done()
		eventFunc(msg, taskIdx)
	}()
}

func (network *P2p) waitTaskDone() {

	waitDone := make(chan struct{})
	go func() {
		defer close(waitDone)
		network.taskGroup.Wait()
	}()
	select {
	case <-waitDone:
	case <-time.After(time.Second * 20):
		log.Error("P2pWaitTaskDone", "err", "20s timeout")
	}
}
