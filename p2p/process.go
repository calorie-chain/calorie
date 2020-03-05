package p2p


type pubFuncType func(interface{}, string)

func (n *Node) pubToPeer(data interface{}, pid string) {
	n.pubsub.FIFOPub(data, pid)
}

func (n *Node) processSendP2P(rawData interface{}, peerVersion int32, pid, peerAddr string) (sendData *types.BroadCastData, doSend bool) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("processSendP2P_Panic", "sendData", rawData, "peerAddr", peerAddr, "recoverErr", r)
			doSend = false
		}
	}()
	log.Debug("ProcessSendP2PBegin", "peerID", pid, "peerAddr", peerAddr)
	sendData = &types.BroadCastData{}
	doSend = false
	if tx, ok := rawData.(*types.P2PTx); ok {
		doSend = n.sendTx(tx, sendData, peerVersion, pid, peerAddr)
	} else if blc, ok := rawData.(*types.P2PBlock); ok {
		doSend = n.sendBlock(blc, sendData, peerVersion, pid, peerAddr)
	} else if query, ok := rawData.(*types.P2PQueryData); ok {
		doSend = n.sendQueryData(query, sendData, peerAddr)
	} else if rep, ok := rawData.(*types.P2PBlockTxReply); ok {
		doSend = n.sendQueryReply(rep, sendData, peerAddr)
	} else if ping, ok := rawData.(*types.P2PPing); ok {
		doSend = true
		sendData.Value = &types.BroadCastData_Ping{Ping: ping}
	}
	log.Debug("ProcessSendP2PEnd", "peerAddr", peerAddr, "doSend", doSend)
	return
}

func (n *Node) processRecvP2P(data *types.BroadCastData, pid string, pubPeerFunc pubFuncType, peerAddr string) (handled bool) {

	defer func() {
		if r := recover(); r != nil {
			log.Error("ProcessRecvP2P_Panic", "recvData", data, "peerAddr", peerAddr, "recoverErr", r)
		}
	}()
	log.Debug("ProcessRecvP2P", "peerID", pid, "peerAddr", peerAddr)
	if pid == "" {
		return false
	}
	handled = true
	if tx := data.GetTx(); tx != nil {
		n.recvTx(tx, pid, peerAddr)
	} else if ltTx := data.GetLtTx(); ltTx != nil {
		n.recvLtTx(ltTx, pid, peerAddr, pubPeerFunc)
	} else if ltBlc := data.GetLtBlock(); ltBlc != nil {
		n.recvLtBlock(ltBlc, pid, peerAddr, pubPeerFunc)
	} else if blc := data.GetBlock(); blc != nil {
		n.recvBlock(blc, pid, peerAddr)
	} else if query := data.GetQuery(); query != nil {
		n.recvQueryData(query, pid, peerAddr, pubPeerFunc)
	} else if rep := data.GetBlockRep(); rep != nil {
		n.recvQueryReply(rep, pid, peerAddr, pubPeerFunc)
	} else {
		handled = false
	}
	log.Debug("ProcessRecvP2P", "peerAddr", peerAddr, "handled", handled)
	return
}

func (n *Node) sendBlock(block *types.P2PBlock, p2pData *types.BroadCastData, peerVersion int32, pid, peerAddr string) (doSend bool) {

	byteHash := block.Block.Hash(n.cfg)
	blockHash := hex.EncodeToString(byteHash)
	ignoreSend := n.addIgnoreSendPeerAtomic(blockSendFilter, blockHash, pid)
	log.Debug("P2PSendBlock", "blockHash", blockHash, "peerIsLtVersion", peerVersion >= lightBroadCastVersion,
		"peerAddr", peerAddr, "ignoreSend", ignoreSend)
	if ignoreSend {
		return false
	}

	if peerVersion >= lightBroadCastVersion && len(block.Block.Txs) >= int(n.nodeInfo.cfg.MinLtBlockTxNum) {

		ltBlock := &types.LightBlock{}
		ltBlock.Size = int64(types.Size(block.Block))
		ltBlock.Header = block.Block.GetHeader(n.cfg)
		ltBlock.Header.Hash = byteHash[:]
		ltBlock.Header.Signature = block.Block.Signature
		ltBlock.MinerTx = block.Block.Txs[0]
		for _, tx := range block.Block.Txs[1:] {
			ltBlock.STxHashes = append(ltBlock.STxHashes, types.CalcTxShortHash(tx.Hash()))
		}

		if !totalBlockCache.contains(blockHash) {
			totalBlockCache.add(blockHash, block.Block, ltBlock.Size)
		}

		p2pData.Value = &types.BroadCastData_LtBlock{LtBlock: ltBlock}
	} else {
		p2pData.Value = &types.BroadCastData_Block{Block: block}
	}

	return true
}

func (n *Node) sendQueryData(query *types.P2PQueryData, p2pData *types.BroadCastData, peerAddr string) bool {
	log.Debug("P2PSendQueryData", "peerAddr", peerAddr)
	p2pData.Value = &types.BroadCastData_Query{Query: query}
	return true
}

func (n *Node) sendQueryReply(rep *types.P2PBlockTxReply, p2pData *types.BroadCastData, peerAddr string) bool {
	log.Debug("P2PSendQueryReply", "peerAddr", peerAddr)
	p2pData.Value = &types.BroadCastData_BlockRep{BlockRep: rep}
	return true
}

func (n *Node) sendTx(tx *types.P2PTx, p2pData *types.BroadCastData, peerVersion int32, pid, peerAddr string) (doSend bool) {

	txHash := hex.EncodeToString(tx.Tx.Hash())
	ttl := tx.GetRoute().GetTTL()
	isLightSend := peerVersion >= lightBroadCastVersion && ttl >= n.nodeInfo.cfg.LightTxTTL
	ignoreSend := false

	if !isLightSend {
		ignoreSend = n.addIgnoreSendPeerAtomic(txSendFilter, txHash, pid)
	}

	log.Debug("P2PSendTx", "txHash", txHash, "ttl", ttl, "isLightSend", isLightSend,
		"peerAddr", peerAddr, "ignoreSend", ignoreSend)

	if ignoreSend {
		return false
	}
	if ttl > n.nodeInfo.cfg.MaxTTL {
		return false
	}

	if isLightSend {
		p2pData.Value = &types.BroadCastData_LtTx{ 
			LtTx: &types.LightTx{
				TxHash: tx.Tx.Hash(),
				Route:  tx.GetRoute(),
			},
		}
	} else {
		p2pData.Value = &types.BroadCastData_Tx{Tx: tx}
	}
	return true
}

func (n *Node) recvTx(tx *types.P2PTx, pid, peerAddr string) {
	if tx.GetTx() == nil {
		return
	}
	txHash := hex.EncodeToString(tx.GetTx().Hash())
	n.addIgnoreSendPeerAtomic(txSendFilter, txHash, pid)
	isDuplicate := n.checkAndRegFilterAtomic(txHashFilter, txHash)
	log.Debug("recvTx", "tx", txHash, "ttl", tx.GetRoute().GetTTL(), "peerAddr", peerAddr, "duplicateTx", isDuplicate)
	if isDuplicate {
		return
	}
	if tx.GetRoute() == nil {
		tx.Route = &types.P2PRoute{TTL: 1}
	}
	txHashFilter.Add(txHash, tx.GetRoute())

	msg := n.nodeInfo.client.NewMessage("mempool", types.EventTx, tx.GetTx())
	errs := n.nodeInfo.client.Send(msg, false)
	if errs != nil {
		log.Error("recvTx", "process EventTx msg Error", errs.Error())
	}

}

func (n *Node) recvLtTx(tx *types.LightTx, pid, peerAddr string, pubPeerFunc pubFuncType) {

	txHash := hex.EncodeToString(tx.TxHash)
	n.addIgnoreSendPeerAtomic(txSendFilter, txHash, pid)
	exist := txHashFilter.QueryRecvData(txHash)
	log.Debug("recvLtTx", "txHash", txHash, "ttl", tx.GetRoute().GetTTL(), "peerAddr", peerAddr, "exist", exist)
	if !exist {

		query := &types.P2PQueryData{}
		query.Value = &types.P2PQueryData_TxReq{
			TxReq: &types.P2PTxReq{
				TxHash: tx.TxHash,
			},
		}
		pubPeerFunc(query, pid)
	}
}

func (n *Node) recvBlock(block *types.P2PBlock, pid, peerAddr string) {

	if block.GetBlock() == nil {
		return
	}
	blockHash := hex.EncodeToString(block.GetBlock().Hash(n.cfg))
	n.addIgnoreSendPeerAtomic(blockSendFilter, blockHash, pid)
	isDuplicate := n.checkAndRegFilterAtomic(blockHashFilter, blockHash)
	log.Debug("recvBlock", "blockHeight", block.GetBlock().GetHeight(), "peerAddr", peerAddr,
		"block size(KB)", float32(block.Block.Size())/1024, "blockHash", blockHash, "duplicateBlock", isDuplicate)
	if isDuplicate {
		return
	}
	if err := n.postBlockChain(block.GetBlock(), pid); err != nil {
		log.Error("recvBlock", "send block to blockchain Error", err.Error())
	}

}

func (n *Node) recvLtBlock(ltBlock *types.LightBlock, pid, peerAddr string, pubPeerFunc pubFuncType) {

	blockHash := hex.EncodeToString(ltBlock.Header.Hash)
	n.addIgnoreSendPeerAtomic(blockSendFilter, blockHash, pid)
	isDuplicate := n.checkAndRegFilterAtomic(blockHashFilter, blockHash)
	log.Debug("recvLtBlock", "blockHash", blockHash, "blockHeight", ltBlock.GetHeader().GetHeight(),
		"peerAddr", peerAddr, "duplicateBlock", isDuplicate)
	if isDuplicate {
		return
	}
	block := &types.Block{}
	block.TxHash = ltBlock.Header.TxHash
	block.Signature = ltBlock.Header.Signature
	block.ParentHash = ltBlock.Header.ParentHash
	block.Height = ltBlock.Header.Height
	block.BlockTime = ltBlock.Header.BlockTime
	block.Difficulty = ltBlock.Header.Difficulty
	block.Version = ltBlock.Header.Version
	block.StateHash = ltBlock.Header.StateHash
	block.Txs = append(block.Txs, ltBlock.MinerTx)

	txList := &types.ReplyTxList{}
	ok := false
	if len(ltBlock.STxHashes) > 0 {
		resp, err := n.queryMempool(types.EventTxListByHash, &types.ReqTxHashList{Hashes: ltBlock.STxHashes, IsShortHash: true})
		if err != nil {
			log.Error("recvLtBlock", "queryTxListByHashErr", err)
			return
		}

		txList, ok = resp.(*types.ReplyTxList)
		if !ok {
			log.Error("recvLtBlock", "queryMemPool", "nilReplyTxList")
		}
	}
	nilTxIndices := make([]int32, 0)
	for i := 0; ok && i < len(txList.Txs); i++ {
		tx := txList.Txs[i]
		if tx == nil {
			nilTxIndices = append(nilTxIndices, int32(i+1))
			tx = &types.Transaction{}
		} else if count := tx.GetGroupCount(); count > 0 {

			group, err := tx.GetTxGroup()
			if err != nil {
				log.Error("recvLtBlock", "getTxGroupErr", err)
				nilTxIndices = nilTxIndices[:0]
				break
			}
			block.Txs = append(block.Txs, group.Txs...)
			i += len(group.Txs) - 1
			continue
		}

		block.Txs = append(block.Txs, tx)
	}
	nilTxLen := len(nilTxIndices)
	if nilTxLen == 0 && len(block.Txs) == int(ltBlock.Header.TxCount) {
		if bytes.Equal(block.TxHash, merkle.CalcMerkleRoot(n.cfg, block.Height, block.Txs)) {
			log.Debug("recvLtBlock", "height", block.GetHeight(), "peerAddr", peerAddr,
				"blockHash", blockHash, "block size(KB)", float32(ltBlock.Size)/1024)
			if err := n.postBlockChain(block, pid); err != nil {
				log.Error("recvLtBlock", "send block to blockchain Error", err.Error())
			}
			return
		}
		log.Debug("recvLtBlock:TxHashCheckFail", "height", block.GetHeight(), "peerAddr", peerAddr,
			"blockHash", blockHash, "block.Txs", block.Txs)
	}
	if nilTxLen > 0 && (float32(nilTxLen) > float32(ltBlock.Header.TxCount)/3 ||
		float32(block.Size()) < float32(ltBlock.Size)/3) {
		nilTxIndices = nilTxIndices[:0]
	}
	log.Debug("recvLtBlock", "queryBlockHash", blockHash, "queryHeight", ltBlock.GetHeader().GetHeight(), "queryTxNum", len(nilTxIndices))

	query := &types.P2PQueryData{
		Value: &types.P2PQueryData_BlockTxReq{
			BlockTxReq: &types.P2PBlockTxReq{
				BlockHash: blockHash,
				TxIndices: nilTxIndices,
			},
		},
	}
	pubPeerFunc(query, pid)
	ltBlockCache.add(blockHash, block, int64(block.Size()))
}

func (n *Node) recvQueryData(query *types.P2PQueryData, pid, peerAddr string, pubPeerFunc pubFuncType) {

	if txReq := query.GetTxReq(); txReq != nil {

		txHash := hex.EncodeToString(txReq.TxHash)
		log.Debug("recvQueryTx", "txHash", txHash, "peerAddr", peerAddr)
		resp, err := n.queryMempool(types.EventTxListByHash, &types.ReqTxHashList{Hashes: []string{string(txReq.TxHash)}})
		if err != nil {
			log.Error("recvQuery", "queryMempoolErr", err)
			return
		}

		p2pTx := &types.P2PTx{}
		txList, ok := resp.(*types.ReplyTxList)
		for i := 0; ok && i < len(txList.Txs); i++ {
			p2pTx.Tx = txList.Txs[i]
		}

		if p2pTx.GetTx() == nil {
			log.Error("recvQueryTx", "txHash", txHash, "err", "recvNilTxFromMempool")
			return
		}
		p2pTx.Route = &types.P2PRoute{TTL: 1}
		pubPeerFunc(p2pTx, pid)

	} else if blcReq := query.GetBlockTxReq(); blcReq != nil {

		log.Debug("recvQueryBlockTx", "blockHash", blcReq.BlockHash, "queryTxCount", len(blcReq.TxIndices), "peerAddr", peerAddr)
		if block, ok := totalBlockCache.get(blcReq.BlockHash).(*types.Block); ok {

			blockRep := &types.P2PBlockTxReply{BlockHash: blcReq.BlockHash}

			blockRep.TxIndices = blcReq.TxIndices
			for _, idx := range blcReq.TxIndices {
				blockRep.Txs = append(blockRep.Txs, block.Txs[idx])
			}
			if len(blockRep.TxIndices) == 0 {
				blockRep.Txs = block.Txs
			}
			pubPeerFunc(blockRep, pid)
		}
	}
}

func (n *Node) recvQueryReply(rep *types.P2PBlockTxReply, pid, peerAddr string, pubPeerFunc pubFuncType) {

	log.Debug("recvQueryReplyBlock", "blockHash", rep.GetBlockHash(), "queryTxsCount", len(rep.GetTxIndices()), "peerAddr", peerAddr)
	val, exist := ltBlockCache.del(rep.BlockHash)
	block, _ := val.(*types.Block)
	if !exist || block == nil {
		return
	}
	for i, idx := range rep.TxIndices {
		block.Txs[idx] = rep.Txs[i]
	}

	if len(rep.TxIndices) == 0 {
		block.Txs = rep.Txs
	}

	if bytes.Equal(block.TxHash, merkle.CalcMerkleRoot(n.cfg, block.Height, block.Txs)) {

		log.Debug("recvQueryReplyBlock", "blockHeight", block.GetHeight(), "peerAddr", peerAddr,
			"block size(KB)", float32(block.Size())/1024, "blockHash", rep.BlockHash)
		if err := n.postBlockChain(block, pid); err != nil {
			log.Error("recvQueryReplyBlock", "send block to blockchain Error", err.Error())
		}
	} else if len(rep.TxIndices) != 0 {
		log.Debug("recvQueryReplyBlock", "GetTotalBlock", block.GetHeight())
		query := &types.P2PQueryData{
			Value: &types.P2PQueryData_BlockTxReq{
				BlockTxReq: &types.P2PBlockTxReq{
					BlockHash: rep.BlockHash,
					TxIndices: nil,
				},
			},
		}
		pubPeerFunc(query, pid)
		block.Txs = nil
		ltBlockCache.add(rep.BlockHash, block, int64(block.Size()))
	}
}

func (n *Node) queryMempool(ty int64, data interface{}) (interface{}, error) {

	client := n.nodeInfo.client

	msg := client.NewMessage("mempool", ty, data)
	err := client.Send(msg, true)
	if err != nil {
		return nil, err
	}
	resp, err := client.WaitTimeout(msg, time.Second*10)
	if err != nil {
		return nil, err
	}
	return resp.Data, nil
}

func (n *Node) postBlockChain(block *types.Block, pid string) error {

	msg := n.nodeInfo.client.NewMessage("blockchain", types.EventBroadcastAddBlock, &types.BlockPid{Pid: pid, Block: block})
	err := n.nodeInfo.client.Send(msg, false)
	if err != nil {
		log.Error("postBlockChain", "send to blockchain Error", err.Error())
		return err
	}
	return nil
}

func (n *Node) checkAndRegFilterAtomic(filter *Filterdata, key string) (exist bool) {

	filter.GetLock()
	defer filter.ReleaseLock()
	if filter.QueryRecvData(key) {
		return true
	}
	filter.RegRecvData(key)
	return false
}

func (n *Node) addIgnoreSendPeerAtomic(filter *Filterdata, key, pid string) (exist bool) {

	filter.GetLock()
	defer filter.ReleaseLock()
	var info *sendFilterInfo
	if !filter.QueryRecvData(key) {
		info = &sendFilterInfo{ignoreSendPeers: make(map[string]bool)}
		filter.Add(key, info)
	} else {
		info = filter.Get(key).(*sendFilterInfo)
	}
	_, exist = info.ignoreSendPeers[pid]
	info.ignoreSendPeers[pid] = true
	return exist
}
