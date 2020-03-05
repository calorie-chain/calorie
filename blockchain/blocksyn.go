package blockchain


var (
	BackBlockNum            int64 = 128                 	BackwardBlockNum        int64 = 16                   	checkHeightNoIncSeconds int64 = 5 * 60               	checkBlockHashSeconds   int64 = 1 * 60              	fetchPeerListSeconds    int64 = 5                    
	MaxRollBlockNum         int64 = 10000               
	ReduceHeight                  = MaxRollBlockNum      
	SafetyReduceHeight            = ReduceHeight * 3 / 2 
	batchsyncblocknum int64 = 5000 
	synlog = chainlog.New("submodule", "syn")
)

type PeerInfo struct {
	Name       string
	ParentHash []byte
	Height     int64
	Hash       []byte
}

type PeerInfoList []*PeerInfo

func (list PeerInfoList) Len() int {
	return len(list)
}

func (list PeerInfoList) Less(i, j int) bool {
	if list[i].Height < list[j].Height {
		return true
	} else if list[i].Height > list[j].Height {
		return false
	} else {
		return list[i].Name < list[j].Name
	}
}

func (list PeerInfoList) Swap(i, j int) {
	temp := list[i]
	list[i] = list[j]
	list[j] = temp
}

type FaultPeerInfo struct {
	Peer        *PeerInfo
	FaultHeight int64
	FaultHash   []byte
	ErrInfo     error
	ReqFlag     bool
}

type BestPeerInfo struct {
	Peer        *PeerInfo
	Height      int64
	Hash        []byte
	Td          *big.Int
	ReqFlag     bool
	IsBestChain bool
}


type BlockOnChain struct {
	sync.RWMutex
	Height      int64
	OnChainTime int64
}

func (chain *BlockChain) initOnChainTimeout() {
	chain.blockOnChain.Lock()
	defer chain.blockOnChain.Unlock()

	chain.blockOnChain.Height = -1
	chain.blockOnChain.OnChainTime = types.Now().Unix()
}

func (chain *BlockChain) OnChainTimeout(height int64) bool {
	chain.blockOnChain.Lock()
	defer chain.blockOnChain.Unlock()

	if chain.onChainTimeout == 0 {
		return false
	}

	curTime := types.Now().Unix()
	if chain.blockOnChain.Height != height {
		chain.blockOnChain.Height = height
		chain.blockOnChain.OnChainTime = curTime
		return false
	}
	if curTime-chain.blockOnChain.OnChainTime > chain.onChainTimeout {
		synlog.Debug("OnChainTimeout", "curTime", curTime, "blockOnChain", chain.blockOnChain)
		return true
	}
	return false
}

func (chain *BlockChain) SynRoutine() {
	fetchPeerListTicker := time.NewTicker(time.Duration(fetchPeerListSeconds) * time.Second)

	blockSynTicker := time.NewTicker(chain.blockSynInterVal * time.Second)

	checkHeightNoIncreaseTicker := time.NewTicker(time.Duration(checkHeightNoIncSeconds) * time.Second)

	checkBlockHashTicker := time.NewTicker(time.Duration(checkBlockHashSeconds) * time.Second)

	checkClockDriftTicker := time.NewTicker(300 * time.Second)

	recoveryFaultPeerTicker := time.NewTicker(180 * time.Second)

	checkBestChainTicker := time.NewTicker(120 * time.Second)

	if chain.GetDownloadSyncStatus() {
		go chain.FastDownLoadBlocks()
	}
	for {
		select {
		case <-chain.quit:
			return
		case <-blockSynTicker.C:
			if !chain.GetDownloadSyncStatus() {
				go chain.SynBlocksFromPeers()
			}

		case <-fetchPeerListTicker.C:
			chain.tickerwg.Add(1)
			go chain.FetchPeerList()

		case <-checkHeightNoIncreaseTicker.C:
			chain.tickerwg.Add(1)
			go chain.CheckHeightNoIncrease()

		case <-checkBlockHashTicker.C:
			chain.tickerwg.Add(1)
			go chain.CheckTipBlockHash()

		case <-checkClockDriftTicker.C:
			go chain.checkClockDrift()

		case <-recoveryFaultPeerTicker.C:
			chain.tickerwg.Add(1)
			go chain.RecoveryFaultPeer()

		case <-checkBestChainTicker.C:
			chain.tickerwg.Add(1)
			go chain.CheckBestChain(false)
		}
	}
}


func (chain *BlockChain) FetchBlock(start int64, end int64, pid []string, syncOrfork bool) (err error) {
	if chain.client == nil {
		synlog.Error("FetchBlock chain client not bind message queue.")
		return types.ErrClientNotBindQueue
	}

	synlog.Debug("FetchBlock input", "StartHeight", start, "EndHeight", end, "pid", pid)
	blockcount := end - start
	if blockcount < 0 {
		return types.ErrStartBigThanEnd
	}
	var requestblock types.ReqBlocks
	requestblock.Start = start
	requestblock.IsDetail = false
	requestblock.Pid = pid

	if blockcount >= chain.MaxFetchBlockNum {
		requestblock.End = start + chain.MaxFetchBlockNum - 1
	} else {
		requestblock.End = end
	}
	var cb func()
	var timeoutcb func(height int64)
	if syncOrfork {
		if requestblock.End < chain.downLoadInfo.EndHeight {
			cb = func() {
				chain.ReqDownLoadBlocks()
			}
			timeoutcb = func(height int64) {
				chain.DownLoadTimeOutProc(height)
			}
			chain.UpdateDownLoadStartHeight(requestblock.End + 1)
			if chain.GetDownloadSyncStatus() {
				chain.UpdateDownLoadPids()
			}
		} else { 			chain.DefaultDownLoadInfo()
		}
		err = chain.downLoadTask.Start(requestblock.Start, requestblock.End, cb, timeoutcb)
		if err != nil {
			return err
		}
	} else {
		if chain.GetPeerMaxBlkHeight()-requestblock.End > BackBlockNum {
			cb = func() {
				chain.SynBlocksFromPeers()
			}
		}
		err = chain.syncTask.Start(requestblock.Start, requestblock.End, cb, timeoutcb)
		if err != nil {
			return err
		}
	}

	synlog.Info("FetchBlock", "Start", requestblock.Start, "End", requestblock.End)
	msg := chain.client.NewMessage("p2p", types.EventFetchBlocks, &requestblock)
	Err := chain.client.Send(msg, true)
	if Err != nil {
		synlog.Error("FetchBlock", "client.Send err:", Err)
		return err
	}
	resp, err := chain.client.Wait(msg)
	if err != nil {
		synlog.Error("FetchBlock", "client.Wait err:", err)
		return err
	}
	return resp.Err()
}

func (chain *BlockChain) FetchPeerList() {
	defer chain.tickerwg.Done()
	err := chain.fetchPeerList()
	if err != nil {
		synlog.Error("FetchPeerList.", "err", err)
	}
}

func (chain *BlockChain) fetchPeerList() error {
	if chain.client == nil {
		synlog.Error("fetchPeerList chain client not bind message queue.")
		return nil
	}
	msg := chain.client.NewMessage("p2p", types.EventPeerInfo, nil)
	Err := chain.client.SendTimeout(msg, true, 30*time.Second)
	if Err != nil {
		synlog.Error("fetchPeerList", "client.Send err:", Err)
		return Err
	}
	resp, err := chain.client.WaitTimeout(msg, 60*time.Second)
	if err != nil {
		synlog.Error("fetchPeerList", "client.Wait err:", err)
		return err
	}

	peerlist := resp.GetData().(*types.PeerList)
	if peerlist == nil {
		synlog.Error("fetchPeerList", "peerlist", "is nil")
		return types.ErrNoPeer
	}
	curheigt := chain.GetBlockHeight()

	var peerInfoList PeerInfoList
	for _, peer := range peerlist.Peers {
		if peer.Self || curheigt > peer.Header.Height+5 {
			continue
		}
		var peerInfo PeerInfo
		peerInfo.Name = peer.Name
		peerInfo.ParentHash = peer.Header.ParentHash
		peerInfo.Height = peer.Header.Height
		peerInfo.Hash = peer.Header.Hash
		peerInfoList = append(peerInfoList, &peerInfo)
	}
	if len(peerInfoList) == 0 {
		return nil
	}
	sort.Sort(peerInfoList)

	subInfoList := peerInfoList

	chain.peerMaxBlklock.Lock()
	chain.peerList = subInfoList
	chain.peerMaxBlklock.Unlock()

	if atomic.LoadInt32(&chain.firstcheckbestchain) == 0 {
		synlog.Info("fetchPeerList trigger first CheckBestChain")
		chain.CheckBestChain(true)
	}
	return nil
}

func (chain *BlockChain) GetRcvLastCastBlkHeight() int64 {
	chain.castlock.Lock()
	defer chain.castlock.Unlock()
	return chain.rcvLastBlockHeight
}

func (chain *BlockChain) UpdateRcvCastBlkHeight(height int64) {
	chain.castlock.Lock()
	defer chain.castlock.Unlock()
	chain.rcvLastBlockHeight = height
}

func (chain *BlockChain) GetsynBlkHeight() int64 {
	chain.synBlocklock.Lock()
	defer chain.synBlocklock.Unlock()
	return chain.synBlockHeight
}

func (chain *BlockChain) UpdatesynBlkHeight(height int64) {
	chain.synBlocklock.Lock()
	defer chain.synBlocklock.Unlock()
	chain.synBlockHeight = height
}

func (chain *BlockChain) GetPeerMaxBlkHeight() int64 {
	chain.peerMaxBlklock.Lock()
	defer chain.peerMaxBlklock.Unlock()

	if chain.peerList != nil {
		peerlen := len(chain.peerList)
		for i := peerlen - 1; i >= 0; i-- {
			if chain.peerList[i] != nil {
				ok := chain.IsFaultPeer(chain.peerList[i].Name)
				if !ok {
					return chain.peerList[i].Height
				}
			}
		}
		maxpeer := chain.peerList[peerlen-1]
		if maxpeer != nil {
			synlog.Debug("GetPeerMaxBlkHeight all peers are faultpeer maybe self on Side chain", "pid", maxpeer.Name, "Height", maxpeer.Height, "Hash", common.ToHex(maxpeer.Hash))
			return maxpeer.Height
		}
	}
	return -1
}

func (chain *BlockChain) GetPeerInfo(pid string) *PeerInfo {
	chain.peerMaxBlklock.Lock()
	defer chain.peerMaxBlklock.Unlock()

	if chain.peerList != nil {
		for _, peer := range chain.peerList {
			if pid == peer.Name {
				return peer
			}
		}
	}
	return nil
}

func (chain *BlockChain) GetMaxPeerInfo() *PeerInfo {
	chain.peerMaxBlklock.Lock()
	defer chain.peerMaxBlklock.Unlock()

	if chain.peerList != nil {
		peerlen := len(chain.peerList)
		for i := peerlen - 1; i >= 0; i-- {
			if chain.peerList[i] != nil {
				ok := chain.IsFaultPeer(chain.peerList[i].Name)
				if !ok {
					return chain.peerList[i]
				}
			}
		}
		maxpeer := chain.peerList[peerlen-1]
		if maxpeer != nil {
			synlog.Debug("GetMaxPeerInfo all peers are faultpeer maybe self on Side chain", "pid", maxpeer.Name, "Height", maxpeer.Height, "Hash", common.ToHex(maxpeer.Hash))
			return maxpeer
		}
	}
	return nil
}

func (chain *BlockChain) GetPeers() PeerInfoList {
	chain.peerMaxBlklock.Lock()
	defer chain.peerMaxBlklock.Unlock()

	var peers PeerInfoList

	if chain.peerList != nil {
		peers = append(peers, chain.peerList...)
	}
	return peers
}

func (chain *BlockChain) GetPeersMap() map[string]bool {
	chain.peerMaxBlklock.Lock()
	defer chain.peerMaxBlklock.Unlock()
	peersmap := make(map[string]bool)

	if chain.peerList != nil {
		for _, peer := range chain.peerList {
			peersmap[peer.Name] = true
		}
	}
	return peersmap
}

func (chain *BlockChain) IsFaultPeer(pid string) bool {
	chain.faultpeerlock.Lock()
	defer chain.faultpeerlock.Unlock()

	return chain.faultPeerList[pid] != nil
}

func (chain *BlockChain) IsErrExecBlock(height int64, hash []byte) (bool, error) {
	chain.faultpeerlock.Lock()
	defer chain.faultpeerlock.Unlock()

	for _, faultpeer := range chain.faultPeerList {
		if faultpeer.FaultHeight == height && bytes.Equal(hash, faultpeer.FaultHash) {
			return true, faultpeer.ErrInfo
		}
	}
	return false, nil
}

func (chain *BlockChain) GetFaultPeer(pid string) *FaultPeerInfo {
	chain.faultpeerlock.Lock()
	defer chain.faultpeerlock.Unlock()

	return chain.faultPeerList[pid]
}

func (chain *BlockChain) RecoveryFaultPeer() {
	chain.faultpeerlock.Lock()
	defer chain.faultpeerlock.Unlock()

	defer chain.tickerwg.Done()

	for pid, faultpeer := range chain.faultPeerList {
		blockhash, err := chain.blockStore.GetBlockHashByHeight(faultpeer.FaultHeight)
		if err == nil {
			if bytes.Equal(faultpeer.FaultHash, blockhash) {
				synlog.Debug("RecoveryFaultPeer ", "Height", faultpeer.FaultHeight, "FaultHash", common.ToHex(faultpeer.FaultHash), "pid", pid)
				delete(chain.faultPeerList, pid)
				continue
			}
		}

		err = chain.FetchBlockHeaders(faultpeer.FaultHeight, faultpeer.FaultHeight, pid)
		if err == nil {
			chain.faultPeerList[pid].ReqFlag = true
		}
		synlog.Debug("RecoveryFaultPeer", "pid", faultpeer.Peer.Name, "FaultHeight", faultpeer.FaultHeight, "FaultHash", common.ToHex(faultpeer.FaultHash), "Err", faultpeer.ErrInfo)
	}
}

func (chain *BlockChain) AddFaultPeer(faultpeer *FaultPeerInfo) {
	chain.faultpeerlock.Lock()
	defer chain.faultpeerlock.Unlock()

	faultnode := chain.faultPeerList[faultpeer.Peer.Name]
	if faultnode != nil {
		synlog.Debug("AddFaultPeer old", "pid", faultnode.Peer.Name, "FaultHeight", faultnode.FaultHeight, "FaultHash", common.ToHex(faultnode.FaultHash), "Err", faultnode.ErrInfo)
	}
	chain.faultPeerList[faultpeer.Peer.Name] = faultpeer
	synlog.Debug("AddFaultPeer new", "pid", faultpeer.Peer.Name, "FaultHeight", faultpeer.FaultHeight, "FaultHash", common.ToHex(faultpeer.FaultHash), "Err", faultpeer.ErrInfo)
}

func (chain *BlockChain) RemoveFaultPeer(pid string) {
	chain.faultpeerlock.Lock()
	defer chain.faultpeerlock.Unlock()
	synlog.Debug("RemoveFaultPeer", "pid", pid)

	delete(chain.faultPeerList, pid)
}

func (chain *BlockChain) UpdateFaultPeer(pid string, reqFlag bool) {
	chain.faultpeerlock.Lock()
	defer chain.faultpeerlock.Unlock()

	faultpeer := chain.faultPeerList[pid]
	if faultpeer != nil {
		faultpeer.ReqFlag = reqFlag
	}
}

func (chain *BlockChain) RecordFaultPeer(pid string, height int64, hash []byte, err error) {

	var faultnode FaultPeerInfo

	peerinfo := chain.GetPeerInfo(pid)
	if peerinfo == nil {
		synlog.Error("RecordFaultPeerNode GetPeerInfo is nil", "pid", pid)
		return
	}
	faultnode.Peer = peerinfo
	faultnode.FaultHeight = height
	faultnode.FaultHash = hash
	faultnode.ErrInfo = err
	faultnode.ReqFlag = false
	chain.AddFaultPeer(&faultnode)
}

func (chain *BlockChain) SynBlocksFromPeers() {

	curheight := chain.GetBlockHeight()
	RcvLastCastBlkHeight := chain.GetRcvLastCastBlkHeight()
	peerMaxBlkHeight := chain.GetPeerMaxBlkHeight()

	if peerMaxBlkHeight > curheight+batchsyncblocknum && !chain.cfgBatchSync {
		atomic.CompareAndSwapInt32(&chain.isbatchsync, 1, 0)
	} else if peerMaxBlkHeight >= 0 {
		atomic.CompareAndSwapInt32(&chain.isbatchsync, 0, 1)
	}

	if chain.syncTask.InProgress() {
		synlog.Info("chain syncTask InProgress")
		return
	}
	if chain.downLoadTask.InProgress() {
		synlog.Info("chain downLoadTask InProgress")
		return
	}
	backWardThanTwo := curheight+1 < peerMaxBlkHeight
	backWardOne := curheight+1 == peerMaxBlkHeight && chain.OnChainTimeout(curheight)

	if backWardThanTwo || backWardOne {
		synlog.Info("SynBlocksFromPeers", "curheight", curheight, "LastCastBlkHeight", RcvLastCastBlkHeight, "peerMaxBlkHeight", peerMaxBlkHeight)
		pids := chain.GetBestChainPids()
		if pids != nil {
			err := chain.FetchBlock(curheight+1, peerMaxBlkHeight, pids, false)
			if err != nil {
				synlog.Error("SynBlocksFromPeers FetchBlock", "err", err)
			}
		} else {
			synlog.Info("SynBlocksFromPeers GetBestChainPids is nil")
		}
	}
}

func (chain *BlockChain) CheckHeightNoIncrease() {
	defer chain.tickerwg.Done()

	tipheight := chain.bestChain.Height()
	laststorheight := chain.blockStore.Height()

	if tipheight != laststorheight {
		synlog.Error("CheckHeightNoIncrease", "tipheight", tipheight, "laststorheight", laststorheight)
		return
	}
	checkheight := chain.GetsynBlkHeight()

	if tipheight != checkheight {
		chain.UpdatesynBlkHeight(tipheight)
		return
	}
	maxpeer := chain.GetMaxPeerInfo()
	if maxpeer == nil {
		synlog.Error("CheckHeightNoIncrease GetMaxPeerInfo is nil")
		return
	}
	peermaxheight := maxpeer.Height
	pid := maxpeer.Name
	var err error
	if peermaxheight > tipheight && (peermaxheight-tipheight) > BackwardBlockNum && !chain.isBestChainPeer(pid) {
		synlog.Debug("CheckHeightNoIncrease", "tipheight", tipheight, "pid", pid)
		if tipheight > BackBlockNum {
			err = chain.FetchBlockHeaders(tipheight-BackBlockNum, tipheight, pid)
		} else {
			err = chain.FetchBlockHeaders(0, tipheight, pid)
		}
		if err != nil {
			synlog.Error("CheckHeightNoIncrease FetchBlockHeaders", "err", err)
		}
	}
}

func (chain *BlockChain) FetchBlockHeaders(start int64, end int64, pid string) (err error) {
	if chain.client == nil {
		synlog.Error("FetchBlockHeaders chain client not bind message queue.")
		return types.ErrClientNotBindQueue
	}

	chainlog.Debug("FetchBlockHeaders", "StartHeight", start, "EndHeight", end, "pid", pid)

	var requestblock types.ReqBlocks
	requestblock.Start = start
	requestblock.End = end
	requestblock.IsDetail = false
	requestblock.Pid = []string{pid}

	msg := chain.client.NewMessage("p2p", types.EventFetchBlockHeaders, &requestblock)
	Err := chain.client.Send(msg, true)
	if Err != nil {
		synlog.Error("FetchBlockHeaders", "client.Send err:", Err)
		return err
	}
	resp, err := chain.client.Wait(msg)
	if err != nil {
		synlog.Error("FetchBlockHeaders", "client.Wait err:", err)
		return err
	}
	return resp.Err()
}

func (chain *BlockChain) ProcBlockHeader(headers *types.Headers, peerid string) error {

	faultPeer := chain.GetFaultPeer(peerid)
	if faultPeer != nil && faultPeer.ReqFlag && faultPeer.FaultHeight == headers.Items[0].Height {
		if !bytes.Equal(headers.Items[0].Hash, faultPeer.FaultHash) {
			chain.RemoveFaultPeer(peerid)
		} else {
			chain.UpdateFaultPeer(peerid, false)
		}
		return nil
	}

	bestchainPeer := chain.GetBestChainPeer(peerid)
	if bestchainPeer != nil && bestchainPeer.ReqFlag && headers.Items[0].Height == bestchainPeer.Height {
		chain.CheckBestChainProc(headers, peerid)
		return nil
	}

	height := headers.Items[0].Height
	header, err := chain.blockStore.GetBlockHeaderByHeight(height)
	if err != nil {
		return err
	}
	if !bytes.Equal(headers.Items[0].Hash, header.Hash) {
		synlog.Info("ProcBlockHeader hash no equal", "height", height, "self hash", common.ToHex(header.Hash), "peer hash", common.ToHex(headers.Items[0].Hash))

		if height > BackBlockNum {
			err = chain.FetchBlockHeaders(height-BackBlockNum, height, peerid)
		} else if height != 0 {
			err = chain.FetchBlockHeaders(0, height, peerid)
		}
		if err != nil {
			synlog.Info("ProcBlockHeader FetchBlockHeaders", "err", err)
		}
	}
	return nil
}

func (chain *BlockChain) ProcBlockHeaders(headers *types.Headers, pid string) error {
	var ForkHeight int64 = -1
	var forkhash []byte
	var err error
	count := len(headers.Items)
	tipheight := chain.bestChain.Height()

	for i := count - 1; i >= 0; i-- {
		exists := chain.bestChain.HaveBlock(headers.Items[i].Hash, headers.Items[i].Height)
		if exists {
			ForkHeight = headers.Items[i].Height
			forkhash = headers.Items[i].Hash
			break
		}
	}
	if ForkHeight == -1 {
		synlog.Error("ProcBlockHeaders do not find fork point ")
		synlog.Error("ProcBlockHeaders start headerinfo", "height", headers.Items[0].Height, "hash", common.ToHex(headers.Items[0].Hash))
		synlog.Error("ProcBlockHeaders end headerinfo", "height", headers.Items[count-1].Height, "hash", common.ToHex(headers.Items[count-1].Hash))

		startheight := headers.Items[0].Height
		if tipheight > startheight && (tipheight-startheight) > MaxRollBlockNum {
			synlog.Error("ProcBlockHeaders Not Roll Back!", "selfheight", tipheight, "RollBackedhieght", startheight)
			return types.ErrNotRollBack
		}
		height := headers.Items[0].Height
		if height > BackBlockNum {
			err = chain.FetchBlockHeaders(height-BackBlockNum, height, pid)
		} else {
			err = chain.FetchBlockHeaders(0, height, pid)
		}
		if err != nil {
			synlog.Info("ProcBlockHeaders FetchBlockHeaders", "err", err)
		}
		return types.ErrContinueBack
	}
	synlog.Info("ProcBlockHeaders find fork point", "height", ForkHeight, "hash", common.ToHex(forkhash))

	peerinfo := chain.GetPeerInfo(pid)
	if peerinfo == nil {
		synlog.Error("ProcBlockHeaders GetPeerInfo is nil", "pid", pid)
		return types.ErrPeerInfoIsNil
	}

	peermaxheight := peerinfo.Height

	if chain.downLoadTask.InProgress() {
		synlog.Info("ProcBlockHeaders downLoadTask.InProgress")
		return nil
	}
	if !chain.GetDownloadSyncStatus() {
		if chain.syncTask.InProgress() {
			err = chain.syncTask.Cancel()
			synlog.Info("ProcBlockHeaders: cancel syncTask start fork process downLoadTask!", "err", err)
		}
		endHeight := peermaxheight
		if tipheight < peermaxheight {
			endHeight = tipheight + 1
		}
		go chain.ProcDownLoadBlocks(ForkHeight, endHeight, []string{pid})
	}
	return nil
}

func (chain *BlockChain) ProcAddBlockHeadersMsg(headers *types.Headers, pid string) error {
	if headers == nil {
		return types.ErrInvalidParam
	}
	count := len(headers.Items)
	synlog.Debug("ProcAddBlockHeadersMsg", "count", count, "pid", pid)
	if count == 1 {
		return chain.ProcBlockHeader(headers, pid)
	}
	return chain.ProcBlockHeaders(headers, pid)

}

func (chain *BlockChain) CheckTipBlockHash() {
	synlog.Debug("CheckTipBlockHash")
	defer chain.tickerwg.Done()

	tipheight := chain.bestChain.Height()
	tiphash := chain.bestChain.Tip().hash
	laststorheight := chain.blockStore.Height()

	if tipheight != laststorheight {
		synlog.Error("CheckTipBlockHash", "tipheight", tipheight, "laststorheight", laststorheight)
		return
	}

	maxpeer := chain.GetMaxPeerInfo()
	if maxpeer == nil {
		synlog.Error("CheckTipBlockHash GetMaxPeerInfo is nil")
		return
	}
	peermaxheight := maxpeer.Height
	pid := maxpeer.Name
	peerhash := maxpeer.Hash
	var Err error
	if peermaxheight > tipheight {
		synlog.Debug("CheckTipBlockHash >", "peermaxheight", peermaxheight, "tipheight", tipheight)
		Err = chain.FetchBlockHeaders(tipheight, tipheight, pid)
	} else if peermaxheight == tipheight {
		if !bytes.Equal(tiphash, peerhash) {
			if tipheight > BackBlockNum {
				synlog.Debug("CheckTipBlockHash ==", "peermaxheight", peermaxheight, "tipheight", tipheight)
				Err = chain.FetchBlockHeaders(tipheight-BackBlockNum, tipheight, pid)
			} else {
				synlog.Debug("CheckTipBlockHash !=", "peermaxheight", peermaxheight, "tipheight", tipheight)
				Err = chain.FetchBlockHeaders(1, tipheight, pid)
			}
		}
	} else {

		header, err := chain.blockStore.GetBlockHeaderByHeight(peermaxheight)
		if err != nil {
			return
		}
		if !bytes.Equal(header.Hash, peerhash) {
			if peermaxheight > BackBlockNum {
				synlog.Debug("CheckTipBlockHash<!=", "peermaxheight", peermaxheight, "tipheight", tipheight)
				Err = chain.FetchBlockHeaders(peermaxheight-BackBlockNum, peermaxheight, pid)
			} else {
				synlog.Debug("CheckTipBlockHash<!=", "peermaxheight", peermaxheight, "tipheight", tipheight)
				Err = chain.FetchBlockHeaders(1, peermaxheight, pid)
			}
		}
	}
	if Err != nil {
		synlog.Error("CheckTipBlockHash FetchBlockHeaders", "err", Err)
	}
}

func (chain *BlockChain) IsCaughtUp() bool {

	height := chain.GetBlockHeight()
	peers := chain.GetPeers()
	if peers == nil {
		synlog.Debug("IsCaughtUp has no peers")
		return chain.cfg.SingleMode
	}

	var maxPeerHeight int64 = -1
	peersNo := 0
	for _, peer := range peers {
		if peer != nil && maxPeerHeight < peer.Height {
			ok := chain.IsFaultPeer(peer.Name)
			if !ok {
				maxPeerHeight = peer.Height
			}
		}
		peersNo++
	}

	isCaughtUp := (height > 0 || types.Since(chain.startTime) > 60*time.Second) && (maxPeerHeight == 0 || (height >= maxPeerHeight && maxPeerHeight != -1))

	synlog.Debug("IsCaughtUp", "IsCaughtUp ", isCaughtUp, "height", height, "maxPeerHeight", maxPeerHeight, "peersNo", peersNo)
	return isCaughtUp
}

func (chain *BlockChain) GetNtpClockSyncStatus() bool {
	chain.ntpClockSynclock.Lock()
	defer chain.ntpClockSynclock.Unlock()
	return chain.isNtpClockSync
}

func (chain *BlockChain) UpdateNtpClockSyncStatus(Sync bool) {
	chain.ntpClockSynclock.Lock()
	defer chain.ntpClockSynclock.Unlock()
	chain.isNtpClockSync = Sync
}

func (chain *BlockChain) CheckBestChain(isFirst bool) {
	if !isFirst {
		defer chain.tickerwg.Done()
	}
	peers := chain.GetPeers()
	if peers == nil {
		synlog.Debug("CheckBestChain has no peers")
		return
	}

	atomic.CompareAndSwapInt32(&chain.firstcheckbestchain, 0, 1)

	tipheight := chain.bestChain.Height()

	chain.bestpeerlock.Lock()
	defer chain.bestpeerlock.Unlock()

	for _, peer := range peers {
		bestpeer := chain.bestChainPeerList[peer.Name]
		if bestpeer != nil {
			bestpeer.Peer = peer
			bestpeer.Height = tipheight
			bestpeer.Hash = nil
			bestpeer.Td = nil
			bestpeer.ReqFlag = true
		} else {
			if peer.Height < tipheight {
				continue
			}
			var newbestpeer BestPeerInfo
			newbestpeer.Peer = peer
			newbestpeer.Height = tipheight
			newbestpeer.Hash = nil
			newbestpeer.Td = nil
			newbestpeer.ReqFlag = true
			newbestpeer.IsBestChain = false
			chain.bestChainPeerList[peer.Name] = &newbestpeer
		}
		synlog.Debug("CheckBestChain FetchBlockHeaders", "height", tipheight, "pid", peer.Name)
		err := chain.FetchBlockHeaders(tipheight, tipheight, peer.Name)
		if err != nil {
			synlog.Error("CheckBestChain FetchBlockHeaders", "height", tipheight, "pid", peer.Name)
		}
	}
}

func (chain *BlockChain) GetBestChainPeer(pid string) *BestPeerInfo {
	chain.bestpeerlock.Lock()
	defer chain.bestpeerlock.Unlock()
	return chain.bestChainPeerList[pid]
}

func (chain *BlockChain) isBestChainPeer(pid string) bool {
	chain.bestpeerlock.Lock()
	defer chain.bestpeerlock.Unlock()
	peer := chain.bestChainPeerList[pid]
	if peer != nil && peer.IsBestChain {
		return true
	}
	return false
}

func (chain *BlockChain) GetBestChainPids() []string {
	var PeerPids []string
	chain.bestpeerlock.Lock()
	defer chain.bestpeerlock.Unlock()

	peersmap := chain.GetPeersMap()
	for key, value := range chain.bestChainPeerList {
		if !peersmap[value.Peer.Name] {
			delete(chain.bestChainPeerList, value.Peer.Name)
			synlog.Debug("GetBestChainPids:delete", "peer", value.Peer.Name)
			continue
		}
		if value.IsBestChain {
			ok := chain.IsFaultPeer(value.Peer.Name)
			if !ok {
				PeerPids = append(PeerPids, key)
			}
		}
	}
	synlog.Debug("GetBestChainPids", "pids", PeerPids)
	return PeerPids
}

func (chain *BlockChain) CheckBestChainProc(headers *types.Headers, pid string) {

	blockhash, err := chain.blockStore.GetBlockHashByHeight(headers.Items[0].Height)
	if err != nil {
		synlog.Debug("CheckBestChainProc GetBlockHashByHeight", "Height", headers.Items[0].Height, "err", err)
		return
	}

	chain.bestpeerlock.Lock()
	defer chain.bestpeerlock.Unlock()

	bestchainpeer := chain.bestChainPeerList[pid]
	if bestchainpeer == nil {
		synlog.Debug("CheckBestChainProc bestChainPeerList is nil", "Height", headers.Items[0].Height, "pid", pid)
		return
	}
	if bestchainpeer.Height == headers.Items[0].Height {
		bestchainpeer.Hash = headers.Items[0].Hash
		bestchainpeer.ReqFlag = false
		if bytes.Equal(headers.Items[0].Hash, blockhash) {
			bestchainpeer.IsBestChain = true
			synlog.Debug("CheckBestChainProc IsBestChain ", "Height", headers.Items[0].Height, "pid", pid)
		} else {
			bestchainpeer.IsBestChain = false
			synlog.Debug("CheckBestChainProc NotBestChain", "Height", headers.Items[0].Height, "pid", pid)
		}
	}
}
