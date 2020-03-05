package blockchain


var (
	blockLastHeight       = []byte("blockLastHeight")
	bodyPrefix            = []byte("Body:")
	LastSequence          = []byte("LastSequence")
	headerPrefix          = []byte("Header:")
	heightToHeaderPrefix  = []byte("HH:")
	hashPrefix            = []byte("Hash:")
	tdPrefix              = []byte("TD:")
	heightToHashKeyPrefix = []byte("Height:")
	seqToHashKey          = []byte("Seq:")
	HashToSeqPrefix       = []byte("HashToSeq:")
	seqCBPrefix           = []byte("SCB:")
	seqCBLastNumPrefix    = []byte("SCBL:")
	paraSeqToHashKey      = []byte("ParaSeq:")
	HashToParaSeqPrefix   = []byte("HashToParaSeq:")
	LastParaSequence      = []byte("LastParaSequence")
	storeLog              = chainlog.New("submodule", "store")
)


func GetLocalDBKeyList() [][]byte {
	return [][]byte{
		blockLastHeight, bodyPrefix, LastSequence, headerPrefix, heightToHeaderPrefix,
		hashPrefix, tdPrefix, heightToHashKeyPrefix, seqToHashKey, HashToSeqPrefix,
		seqCBPrefix, seqCBLastNumPrefix, tempBlockKey, lastTempBlockKey, LastParaSequence,
		chainParaTxPrefix, chainBodyPrefix, chainHeaderPrefix, chainReceiptPrefix,
	}
}

func calcHashToBlockBodyKey(hash []byte) []byte {
	return append(bodyPrefix, hash...)
}

func calcSeqCBKey(name []byte) []byte {
	return append(append([]byte{}, seqCBPrefix...), name...)
}

func calcSeqCBLastNumKey(name []byte) []byte {
	return append(append([]byte{}, seqCBLastNumPrefix...), name...)
}

func calcHashToBlockHeaderKey(hash []byte) []byte {
	return append(headerPrefix, hash...)
}

func calcHeightToBlockHeaderKey(height int64) []byte {
	return append(heightToHeaderPrefix, []byte(fmt.Sprintf("%012d", height))...)
}

func calcHashToHeightKey(hash []byte) []byte {
	return append(hashPrefix, hash...)
}

func calcHashToTdKey(hash []byte) []byte {
	return append(tdPrefix, hash...)
}

func calcHeightToHashKey(height int64) []byte {
	return append(heightToHashKeyPrefix, []byte(fmt.Sprintf("%v", height))...)
}

func calcSequenceToHashKey(sequence int64, isPara bool) []byte {
	if isPara {
		return append(paraSeqToHashKey, []byte(fmt.Sprintf("%v", sequence))...)
	}
	return append(seqToHashKey, []byte(fmt.Sprintf("%v", sequence))...)
}

func calcHashToSequenceKey(hash []byte, isPara bool) []byte {
	if isPara {
		return append(HashToParaSeqPrefix, hash...)
	}
	return append(HashToSeqPrefix, hash...)
}

func calcLastSeqKey(isPara bool) []byte {
	if isPara {
		return LastParaSequence
	}
	return LastSequence
}

func calcHashToMainSequenceKey(hash []byte) []byte {
	return append(HashToSeqPrefix, hash...)
}

func calcMainSequenceToHashKey(sequence int64) []byte {
	return append(seqToHashKey, []byte(fmt.Sprintf("%v", sequence))...)
}

type BlockStore struct {
	db             dbm.DB
	client         queue.Client
	height         int64
	lastBlock      *types.Block
	lastheaderlock sync.Mutex
	saveSequence   bool
	isParaChain    bool
}

func NewBlockStore(chain *BlockChain, db dbm.DB, client queue.Client) *BlockStore {
	height, err := LoadBlockStoreHeight(db)
	if err != nil {
		chainlog.Info("init::LoadBlockStoreHeight::database may be crash", "err", err.Error())
		if err != types.ErrHeightNotExist {
			panic(err)
		}
	}
	blockStore := &BlockStore{
		height: height,
		db:     db,
		client: client,
	}
	if chain != nil {
		blockStore.saveSequence = chain.isRecordBlockSequence
		blockStore.isParaChain = chain.isParaChain
	}
	cfg := chain.client.GetConfig()
	if height == -1 {
		chainlog.Info("load block height error, may be init database", "height", height)
		if cfg.IsEnable("quickIndex") {
			blockStore.saveQuickIndexFlag()
		}
	} else {
		blockdetail, err := blockStore.LoadBlockByHeight(height)
		if err != nil {
			chainlog.Error("init::LoadBlockByHeight::database may be crash")
			panic(err)
		}
		blockStore.lastBlock = blockdetail.GetBlock()
		flag, err := blockStore.loadFlag(types.FlagTxQuickIndex)
		if err != nil {
			panic(err)
		}
		if cfg.IsEnable("quickIndex") {
			if flag == 0 {
				blockStore.initQuickIndex(height)
			}
		} else {
			if flag != 0 {
				panic("toml config disable tx quick index, but database enable quick index")
			}
		}
	}
	return blockStore
}


func (bs *BlockStore) initQuickIndex(height int64) {
	storeLog.Info("quickIndex upgrade start", "current height", height)
	batch := bs.db.NewBatch(true)
	var maxsize = 100 * 1024 * 1024
	var count = 0
	cfg := bs.client.GetConfig()
	for i := int64(0); i <= height; i++ {
		blockdetail, err := bs.LoadBlockByHeight(i)
		if err != nil {
			panic(err)
		}
		for _, tx := range blockdetail.Block.Txs {
			hash := tx.Hash()
			txresult, err := bs.db.Get(hash)
			if err != nil {
				panic(err)
			}
			count += len(txresult)
			batch.Set(cfg.CalcTxKey(hash), txresult)
			batch.Set(types.CalcTxShortKey(hash), []byte("1"))
		}
		if count > maxsize {
			storeLog.Info("initQuickIndex", "height", i)
			err := batch.Write()
			if err != nil {
				panic(err)
			}
			batch.Reset()
			count = 0
		}
	}
	if count > 0 {
		err := batch.Write()
		if err != nil {
			panic(err)
		}
		storeLog.Info("initQuickIndex", "height", height)
		batch.Reset()
	}
	bs.saveQuickIndexFlag()
}




func (bs *BlockStore) SetSync(key, value []byte) error {
	return bs.db.SetSync(key, value)
}

func (bs *BlockStore) Set(key, value []byte) error {
	return bs.db.Set(key, value)
}

func (bs *BlockStore) GetKey(key []byte) ([]byte, error) {
	value, err := bs.db.Get(key)
	if err != nil && err != dbm.ErrNotFoundInDb {
		return nil, types.ErrNotFound

	}
	return value, err
}

func (bs *BlockStore) PrefixCount(prefix []byte) int64 {
	counts := dbm.NewListHelper(bs.db).PrefixCount(prefix)
	return counts
}

func (bs *BlockStore) List(prefix []byte) ([][]byte, error) {
	values := dbm.NewListHelper(bs.db).PrefixScan(prefix)
	if values == nil {
		return nil, types.ErrNotFound
	}
	return values, nil
}

func (bs *BlockStore) delAllKeys() {
	var allkeys [][]byte
	allkeys = append(allkeys, GetLocalDBKeyList()...)
	allkeys = append(allkeys, version.GetLocalDBKeyList()...)
	allkeys = append(allkeys, types.GetLocalDBKeyList()...)
	var lastkey []byte
	isvalid := true
	for isvalid {
		lastkey, isvalid = bs.delKeys(lastkey, allkeys)
	}
}

func (bs *BlockStore) delKeys(seek []byte, allkeys [][]byte) ([]byte, bool) {
	it := bs.db.Iterator(seek, types.EmptyValue, false)
	defer it.Close()
	i := 0
	count := 0
	var lastkey []byte
	for it.Rewind(); it.Valid(); it.Next() {
		key := it.Key()
		lastkey = key
		if it.Error() != nil {
			panic(it.Error())
		}
		has := false
		for _, prefix := range allkeys {
			if bytes.HasPrefix(key, prefix) {
				has = true
				break
			}
		}
		if !has {
			i++
			if i > 0 && i%10000 == 0 {
				chainlog.Info("del key count", "count", i)
			}
			err := bs.db.Delete(key)
			if err != nil {
				panic(err)
			}
		}
		count++
		if count == 1000000 {
			break
		}
	}
	return lastkey, it.Valid()
}

func (bs *BlockStore) saveQuickIndexFlag() {
	kv := types.FlagKV(types.FlagTxQuickIndex, 1)
	err := bs.db.Set(kv.Key, kv.Value)
	if err != nil {
		panic(err)
	}
}

func (bs *BlockStore) loadFlag(key []byte) (int64, error) {
	flag := &types.Int64{}
	flagBytes, err := bs.db.Get(key)
	if err == nil {
		err = types.Decode(flagBytes, flag)
		if err != nil {
			return 0, err
		}
		return flag.GetData(), nil
	} else if err == types.ErrNotFound || err == dbm.ErrNotFoundInDb {
		return 0, nil
	}
	return 0, err
}

func (bs *BlockStore) HasTx(key []byte) (bool, error) {
	cfg := bs.client.GetConfig()
	if cfg.IsEnable("quickIndex") {
		if _, err := bs.db.Get(types.CalcTxShortKey(key)); err != nil {
			if err == dbm.ErrNotFoundInDb {
				return false, nil
			}
			return false, err
		}
	}
	if _, err := bs.db.Get(cfg.CalcTxKey(key)); err != nil {
		if err == dbm.ErrNotFoundInDb {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (bs *BlockStore) Height() int64 {
	return atomic.LoadInt64(&bs.height)
}

func (bs *BlockStore) UpdateHeight() {
	height, err := LoadBlockStoreHeight(bs.db)
	if err != nil && err != types.ErrHeightNotExist {
		storeLog.Error("UpdateHeight", "LoadBlockStoreHeight err", err)
		return
	}
	atomic.StoreInt64(&bs.height, height)
	storeLog.Debug("UpdateHeight", "curblockheight", height)
}

func (bs *BlockStore) UpdateHeight2(height int64) {
	atomic.StoreInt64(&bs.height, height)
	storeLog.Debug("UpdateHeight2", "curblockheight", height)
}

func (bs *BlockStore) LastHeader() *types.Header {
	bs.lastheaderlock.Lock()
	defer bs.lastheaderlock.Unlock()

	var blockheader = types.Header{}
	if bs.lastBlock != nil {
		blockheader.Version = bs.lastBlock.Version
		blockheader.ParentHash = bs.lastBlock.ParentHash
		blockheader.TxHash = bs.lastBlock.TxHash
		blockheader.StateHash = bs.lastBlock.StateHash
		blockheader.Height = bs.lastBlock.Height
		blockheader.BlockTime = bs.lastBlock.BlockTime
		blockheader.Signature = bs.lastBlock.Signature
		blockheader.Difficulty = bs.lastBlock.Difficulty

		blockheader.Hash = bs.lastBlock.Hash(bs.client.GetConfig())
		blockheader.TxCount = int64(len(bs.lastBlock.Txs))
	}
	return &blockheader
}

func (bs *BlockStore) UpdateLastBlock(hash []byte) {
	blockdetail, err := bs.LoadBlockByHash(hash)
	if err != nil {
		storeLog.Error("UpdateLastBlock", "hash", common.ToHex(hash), "error", err)
		return
	}
	bs.lastheaderlock.Lock()
	defer bs.lastheaderlock.Unlock()
	if blockdetail != nil {
		bs.lastBlock = blockdetail.Block
	}
	storeLog.Debug("UpdateLastBlock", "UpdateLastBlock", blockdetail.Block.Height, "LastHederhash", common.ToHex(blockdetail.Block.Hash(bs.client.GetConfig())))
}

func (bs *BlockStore) UpdateLastBlock2(block *types.Block) {
	bs.lastheaderlock.Lock()
	defer bs.lastheaderlock.Unlock()
	bs.lastBlock = block
	storeLog.Debug("UpdateLastBlock", "UpdateLastBlock", block.Height, "LastHederhash", common.ToHex(block.Hash(bs.client.GetConfig())))
}

func (bs *BlockStore) LastBlock() *types.Block {
	bs.lastheaderlock.Lock()
	defer bs.lastheaderlock.Unlock()
	if bs.lastBlock != nil {
		return bs.lastBlock
	}
	return nil
}

func (bs *BlockStore) Get(keys *types.LocalDBGet) *types.LocalReplyValue {
	var reply types.LocalReplyValue
	for i := 0; i < len(keys.Keys); i++ {
		key := keys.Keys[i]
		value, err := bs.db.Get(key)
		if err != nil && err != types.ErrNotFound {
			storeLog.Error("Get", "error", err)
		}
		reply.Values = append(reply.Values, value)
	}
	return &reply
}


func (bs *BlockStore) LoadBlockByHeight(height int64) (*types.BlockDetail, error) {
	hash, err := bs.GetBlockHashByHeight(height)
	if err != nil {
		return nil, err
	}

	block, err := bs.loadBlockByIndex("", calcHeightHashKey(height, hash), nil)
	if block == nil && err != nil {
		return bs.loadBlockByHashOld(hash)
	}
	return block, nil
}

func (bs *BlockStore) LoadBlockByHash(hash []byte) (*types.BlockDetail, error) {
	block, _, err := bs.loadBlockByHash(hash)
	return block, err
}

func (bs *BlockStore) loadBlockByHash(hash []byte) (*types.BlockDetail, int, error) {
	var blockSize int

	blockdetail, err := bs.loadBlockByIndex("hash", hash, nil)
	if err != nil {
		storeLog.Error("loadBlockByHash:loadBlockByIndex", "hash", common.ToHex(hash), "err", err)
		return nil, blockSize, err
	}
	blockSize = blockdetail.Size()
	return blockdetail, blockSize, nil
}

func (bs *BlockStore) SaveBlock(storeBatch dbm.Batch, blockdetail *types.BlockDetail, sequence int64) (int64, error) {
	cfg := bs.client.GetConfig()
	var lastSequence int64 = -1
	height := blockdetail.Block.Height
	if len(blockdetail.Receipts) == 0 && len(blockdetail.Block.Txs) != 0 {
		storeLog.Error("SaveBlock Receipts is nil ", "height", height)
	}
	hash := blockdetail.Block.Hash(cfg)
	err := bs.saveBlockForTable(storeBatch, blockdetail, true, true)
	if err != nil {
		storeLog.Error("SaveBlock:saveBlockForTable", "height", height, "hash", common.ToHex(hash), "error", err)
		return lastSequence, err
	}
	heightbytes := types.Encode(&types.Int64{Data: height})
	storeBatch.Set(blockLastHeight, heightbytes)

	storeBatch.Set(calcHashToHeightKey(hash), heightbytes)

	storeBatch.Set(calcHeightToHashKey(height), hash)

	if bs.saveSequence || bs.isParaChain {
		lastSequence, err = bs.saveBlockSequence(storeBatch, hash, height, types.AddBlock, sequence)
		if err != nil {
			storeLog.Error("SaveBlock SaveBlockSequence", "height", height, "hash", common.ToHex(hash), "error", err)
			return lastSequence, err
		}
	}
	storeLog.Debug("SaveBlock success", "blockheight", height, "hash", common.ToHex(hash))

	return lastSequence, nil
}

func (bs *BlockStore) BlockdetailToBlockBody(blockdetail *types.BlockDetail) *types.BlockBody {
	cfg := bs.client.GetConfig()
	height := blockdetail.Block.Height
	hash := blockdetail.Block.Hash(cfg)
	var blockbody types.BlockBody
	blockbody.Txs = blockdetail.Block.Txs
	blockbody.Receipts = blockdetail.Receipts
	blockbody.MainHash = hash
	blockbody.MainHeight = height
	blockbody.Hash = hash
	blockbody.Height = height
	if bs.isParaChain {
		blockbody.MainHash = blockdetail.Block.MainHash
		blockbody.MainHeight = blockdetail.Block.MainHeight
	}
	return &blockbody
}

func (bs *BlockStore) DelBlock(storeBatch dbm.Batch, blockdetail *types.BlockDetail, sequence int64) (int64, error) {
	var lastSequence int64 = -1
	height := blockdetail.Block.Height
	hash := blockdetail.Block.Hash(bs.client.GetConfig())

	bytes := types.Encode(&types.Int64{Data: height - 1})
	storeBatch.Set(blockLastHeight, bytes)

	storeBatch.Delete(calcHashToHeightKey(hash))

	storeBatch.Delete(calcHeightToHashKey(height))

	if bs.saveSequence || bs.isParaChain {
		lastSequence, err := bs.saveBlockSequence(storeBatch, hash, height, types.DelBlock, sequence)
		if err != nil {
			storeLog.Error("DelBlock SaveBlockSequence", "height", height, "hash", common.ToHex(hash), "error", err)
			return lastSequence, err
		}
	}
	if !bs.isParaChain {
		parakvs, _ := delParaTxTable(bs.db, height)
		for _, kv := range parakvs {
			if len(kv.GetKey()) != 0 && kv.GetValue() == nil {
				storeBatch.Delete(kv.GetKey())
			}
		}
	}
	storeLog.Debug("DelBlock success", "blockheight", height, "hash", common.ToHex(hash))
	return lastSequence, nil
}

func (bs *BlockStore) GetTx(hash []byte) (*types.TxResult, error) {
	if len(hash) == 0 {
		err := errors.New("input hash is null")
		return nil, err
	}
	cfg := bs.client.GetConfig()
	rawBytes, err := bs.db.Get(cfg.CalcTxKey(hash))
	if rawBytes == nil || err != nil {
		if err != dbm.ErrNotFoundInDb {
			storeLog.Error("GetTx", "hash", common.ToHex(hash), "err", err)
		}
		err = errors.New("tx not exist")
		return nil, err
	}

	var txResult types.TxResult
	err = proto.Unmarshal(rawBytes, &txResult)
	if err != nil {
		return nil, err
	}
	return bs.getRealTxResult(&txResult), nil
}

func (bs *BlockStore) getRealTxResult(txr *types.TxResult) *types.TxResult {
	cfg := bs.client.GetConfig()
	if !cfg.IsEnable("reduceLocaldb") {
		return txr
	}
	blockinfo, err := bs.LoadBlockByHeight(txr.Height)
	if err != nil {
		chainlog.Error("getRealTxResult LoadBlockByHeight", "height", txr.Height, "error", err)
		return txr
	}
	if int(txr.Index) < len(blockinfo.Block.Txs) {
		txr.Tx = blockinfo.Block.Txs[txr.Index]
	}
	if int(txr.Index) < len(blockinfo.Receipts) {
		txr.Receiptdate = blockinfo.Receipts[txr.Index]
	}
	return txr
}

func (bs *BlockStore) AddTxs(storeBatch dbm.Batch, blockDetail *types.BlockDetail) error {
	kv, err := bs.getLocalKV(blockDetail)
	if err != nil {
		storeLog.Error("indexTxs getLocalKV err", "Height", blockDetail.Block.Height, "err", err)
		return err
	}
	for i := 0; i < len(kv.KV); i++ {
		if kv.KV[i].Value == nil {
			storeBatch.Delete(kv.KV[i].Key)
		} else {
			storeBatch.Set(kv.KV[i].Key, kv.KV[i].Value)
		}
	}
	return nil
}

func (bs *BlockStore) DelTxs(storeBatch dbm.Batch, blockDetail *types.BlockDetail) error {
	
	kv, err := bs.getDelLocalKV(blockDetail)
	if err != nil {
		storeLog.Error("indexTxs getLocalKV err", "Height", blockDetail.Block.Height, "err", err)
		return err
	}
	for i := 0; i < len(kv.KV); i++ {
		if kv.KV[i].Value == nil {
			storeBatch.Delete(kv.KV[i].Key)
		} else {
			storeBatch.Set(kv.KV[i].Key, kv.KV[i].Value)
		}
	}

	return nil
}

func (bs *BlockStore) GetHeightByBlockHash(hash []byte) (int64, error) {

	heightbytes, err := bs.db.Get(calcHashToHeightKey(hash))
	if heightbytes == nil || err != nil {
		if err != dbm.ErrNotFoundInDb {
			storeLog.Error("GetHeightByBlockHash", "error", err)
		}
		return -1, types.ErrHashNotExist
	}
	return decodeHeight(heightbytes)
}

func decodeHeight(heightbytes []byte) (int64, error) {
	var height types.Int64
	err := types.Decode(heightbytes, &height)
	if err != nil {
		err = json.Unmarshal(heightbytes, &height.Data)
		if err != nil {
			storeLog.Error("GetHeightByBlockHash Could not unmarshal height bytes", "error", err)
			return -1, types.ErrUnmarshal
		}
	}
	return height.Data, nil
}

func (bs *BlockStore) GetBlockHashByHeight(height int64) ([]byte, error) {

	hash, err := bs.db.Get(calcHeightToHashKey(height))
	if hash == nil || err != nil {
		if err != dbm.ErrNotFoundInDb {
			storeLog.Error("GetBlockHashByHeight", "error", err)
		}
		return nil, types.ErrHeightNotExist
	}
	return hash, nil
}

func (bs *BlockStore) GetBlockHeaderByHeight(height int64) (*types.Header, error) {
	return bs.loadHeaderByIndex(height)
}

func (bs *BlockStore) GetBlockHeaderByHash(hash []byte) (*types.Header, error) {
	header, err := getHeaderByIndex(bs.db, "hash", hash, nil)
	if header == nil || err != nil {
		if err != dbm.ErrNotFoundInDb {
			storeLog.Error("GetBlockHerderByHash:getHeaderByIndex ", "err", err)
		}
		return nil, types.ErrHashNotExist
	}
	return header, nil
}

func (bs *BlockStore) getLocalKV(detail *types.BlockDetail) (*types.LocalDBSet, error) {
	if bs.client == nil {
		panic("client not bind message queue.")
	}
	msg := bs.client.NewMessage("execs", types.EventAddBlock, detail)
	err := bs.client.Send(msg, true)
	if err != nil {
		return nil, err
	}
	resp, err := bs.client.Wait(msg)
	if err != nil {
		return nil, err
	}
	kv := resp.GetData().(*types.LocalDBSet)
	return kv, nil
}

func (bs *BlockStore) getDelLocalKV(detail *types.BlockDetail) (*types.LocalDBSet, error) {
	if bs.client == nil {
		panic("client not bind message queue.")
	}
	msg := bs.client.NewMessage("execs", types.EventDelBlock, detail)
	err := bs.client.Send(msg, true)
	if err != nil {
		return nil, err
	}
	resp, err := bs.client.Wait(msg)
	if err != nil {
		return nil, err
	}
	localDBSet := resp.GetData().(*types.LocalDBSet)
	return localDBSet, nil
}

func (bs *BlockStore) GetTdByBlockHash(hash []byte) (*big.Int, error) {

	blocktd, err := bs.db.Get(calcHashToTdKey(hash))
	if blocktd == nil || err != nil {
		if err != dbm.ErrNotFoundInDb {
			storeLog.Error("GetTdByBlockHash ", "error", err)
		}
		return nil, types.ErrHashNotExist
	}
	td := new(big.Int)
	return td.SetBytes(blocktd), nil
}

func (bs *BlockStore) SaveTdByBlockHash(storeBatch dbm.Batch, hash []byte, td *big.Int) error {
	if td == nil {
		return types.ErrInvalidParam
	}

	storeBatch.Set(calcHashToTdKey(hash), td.Bytes())
	return nil
}

func (bs *BlockStore) NewBatch(sync bool) dbm.Batch {
	storeBatch := bs.db.NewBatch(sync)
	return storeBatch
}

func LoadBlockStoreHeight(db dbm.DB) (int64, error) {
	bytes, err := db.Get(blockLastHeight)
	if bytes == nil || err != nil {
		if err != dbm.ErrNotFoundInDb {
			storeLog.Error("LoadBlockStoreHeight", "error", err)
		}
		return -1, types.ErrHeightNotExist
	}
	return decodeHeight(bytes)
}

func (bs *BlockStore) dbMaybeStoreBlock(blockdetail *types.BlockDetail, sync bool) error {
	if blockdetail == nil {
		return types.ErrInvalidParam
	}
	height := blockdetail.Block.GetHeight()
	hash := blockdetail.Block.Hash(bs.client.GetConfig())
	storeBatch := bs.NewBatch(sync)

	err := bs.saveBlockForTable(storeBatch, blockdetail, false, true)
	if err != nil {
		chainlog.Error("dbMaybeStoreBlock:saveBlockForTable", "height", height, "hash", common.ToHex(hash), "err", err)
		return err
	}
	parentHash := blockdetail.Block.ParentHash

	difficulty := difficulty.CalcWork(blockdetail.Block.Difficulty)

	var blocktd *big.Int
	if height == 0 {
		blocktd = difficulty
	} else {
		parenttd, err := bs.GetTdByBlockHash(parentHash)
		if err != nil {
			chainlog.Error("dbMaybeStoreBlock GetTdByBlockHash", "height", height, "parentHash", common.ToHex(parentHash))
			return err
		}
		blocktd = new(big.Int).Add(difficulty, parenttd)
	}

	err = bs.SaveTdByBlockHash(storeBatch, blockdetail.Block.Hash(bs.client.GetConfig()), blocktd)
	if err != nil {
		chainlog.Error("dbMaybeStoreBlock SaveTdByBlockHash:", "height", height, "hash", common.ToHex(hash), "err", err)
		return err
	}

	err = storeBatch.Write()
	if err != nil {
		chainlog.Error("dbMaybeStoreBlock storeBatch.Write:", "err", err)
		panic(err)
	}
	return nil
}

func (bs *BlockStore) LoadBlockLastSequence() (int64, error) {
	lastKey := calcLastSeqKey(bs.isParaChain)
	bytes, err := bs.db.Get(lastKey)
	if bytes == nil || err != nil {
		if err != dbm.ErrNotFoundInDb {
			storeLog.Error("LoadBlockLastSequence", "error", err)
		}
		return -1, types.ErrHeightNotExist
	}
	return decodeHeight(bytes)
}

func (bs *BlockStore) LoadBlockLastMainSequence() (int64, error) {
	bytes, err := bs.db.Get(LastSequence)
	if bytes == nil || err != nil {
		if err != dbm.ErrNotFoundInDb {
			storeLog.Error("LoadBlockLastMainSequence", "error", err)
		}
		return -1, types.ErrHeightNotExist
	}
	return decodeHeight(bytes)
}


func (bs *BlockStore) saveBlockSequence(storeBatch dbm.Batch, hash []byte, height int64, Type int64, sequence int64) (int64, error) {
	var newSequence int64
	if bs.saveSequence {
		Sequence, err := bs.LoadBlockLastSequence()
		if err != nil {
			storeLog.Error("SaveBlockSequence", "LoadBlockLastSequence err", err)
			if err != types.ErrHeightNotExist {
				panic(err)
			}
		}

		newSequence = Sequence + 1
		if newSequence == 0 && height != 0 {
			storeLog.Error("isRecordBlockSequence is true must Synchronizing data from zero block", "height", height, "seq", newSequence)
			panic(errors.New("isRecordBlockSequence is true must Synchronizing data from zero block"))
		}

		var blockSequence types.BlockSequence
		blockSequence.Hash = hash
		blockSequence.Type = Type
		BlockSequenceByte, err := proto.Marshal(&blockSequence)
		if err != nil {
			storeLog.Error("SaveBlockSequence Marshal BlockSequence", "hash", common.ToHex(hash), "error", err)
			return newSequence, err
		}
		storeBatch.Set(calcSequenceToHashKey(newSequence, bs.isParaChain), BlockSequenceByte)

		sequenceBytes := types.Encode(&types.Int64{Data: newSequence})
		if Type == types.AddBlock {
			storeBatch.Set(calcHashToSequenceKey(hash, bs.isParaChain), sequenceBytes)
		}

		storeBatch.Set(calcLastSeqKey(bs.isParaChain), sequenceBytes)
	}

	if !bs.isParaChain {
		return newSequence, nil
	}

	mainSeq := sequence
	var blockSequence types.BlockSequence
	blockSequence.Hash = hash
	blockSequence.Type = Type
	BlockSequenceByte, err := proto.Marshal(&blockSequence)
	if err != nil {
		storeLog.Error("SaveBlockSequence Marshal BlockSequence", "hash", common.ToHex(hash), "error", err)
		return newSequence, err
	}
	storeBatch.Set(calcMainSequenceToHashKey(mainSeq), BlockSequenceByte)

	sequenceBytes := types.Encode(&types.Int64{Data: mainSeq})
	if Type == types.AddBlock {
		storeBatch.Set(calcHashToMainSequenceKey(hash), sequenceBytes)
	}
	storeBatch.Set(LastSequence, sequenceBytes)

	return newSequence, nil
}

func (bs *BlockStore) LoadBlockBySequence(Sequence int64) (*types.BlockDetail, int, error) {
	BlockSequence, err := bs.GetBlockSequence(Sequence)
	if err != nil {
		return nil, 0, err
	}
	return bs.loadBlockByHash(BlockSequence.Hash)
}

func (bs *BlockStore) LoadBlockByMainSequence(sequence int64) (*types.BlockDetail, int, error) {
	BlockSequence, err := bs.GetBlockByMainSequence(sequence)
	if err != nil {
		return nil, 0, err
	}
	return bs.loadBlockByHash(BlockSequence.Hash)
}

func (bs *BlockStore) GetBlockSequence(Sequence int64) (*types.BlockSequence, error) {
	var blockSeq types.BlockSequence
	blockSeqByte, err := bs.db.Get(calcSequenceToHashKey(Sequence, bs.isParaChain))
	if blockSeqByte == nil || err != nil {
		if err != dbm.ErrNotFoundInDb {
			storeLog.Error("GetBlockSequence", "error", err)
		}
		return nil, types.ErrHeightNotExist
	}

	err = proto.Unmarshal(blockSeqByte, &blockSeq)
	if err != nil {
		storeLog.Error("GetBlockSequence", "err", err)
		return nil, err
	}
	return &blockSeq, nil
}

func (bs *BlockStore) GetBlockByMainSequence(sequence int64) (*types.BlockSequence, error) {
	var blockSeq types.BlockSequence
	blockSeqByte, err := bs.db.Get(calcMainSequenceToHashKey(sequence))
	if blockSeqByte == nil || err != nil {
		if err != dbm.ErrNotFoundInDb {
			storeLog.Error("GetBlockByMainSequence", "error", err)
		}
		return nil, types.ErrHeightNotExist
	}

	err = proto.Unmarshal(blockSeqByte, &blockSeq)
	if err != nil {
		storeLog.Error("GetBlockByMainSequence", "err", err)
		return nil, err
	}
	return &blockSeq, nil
}

func (bs *BlockStore) GetSequenceByHash(hash []byte) (int64, error) {
	var seq types.Int64
	seqbytes, err := bs.db.Get(calcHashToSequenceKey(hash, bs.isParaChain))
	if seqbytes == nil || err != nil {
		if err != dbm.ErrNotFoundInDb {
			storeLog.Error("GetSequenceByHash", "error", err)
		}
		return -1, types.ErrHashNotExist
	}

	err = types.Decode(seqbytes, &seq)
	if err != nil {
		storeLog.Error("GetSequenceByHash  types.Decode", "error", err)
		return -1, types.ErrUnmarshal
	}
	return seq.Data, nil
}

func (bs *BlockStore) GetMainSequenceByHash(hash []byte) (int64, error) {
	var seq types.Int64
	seqbytes, err := bs.db.Get(calcHashToMainSequenceKey(hash))
	if seqbytes == nil || err != nil {
		if err != dbm.ErrNotFoundInDb {
			storeLog.Error("GetMainSequenceByHash", "error", err)
		}
		return -1, types.ErrHashNotExist
	}

	err = types.Decode(seqbytes, &seq)
	if err != nil {
		storeLog.Error("GetMainSequenceByHash  types.Decode", "error", err)
		return -1, types.ErrUnmarshal
	}
	return seq.Data, nil
}

func (bs *BlockStore) GetDbVersion() int64 {
	ver := types.Int64{}
	version, err := bs.db.Get(version.BlockChainVerKey)
	if err != nil && err != types.ErrNotFound {
		storeLog.Info("GetDbVersion", "err", err)
		return 0
	}
	if len(version) == 0 {
		storeLog.Info("GetDbVersion len(version)==0")
		return 0
	}
	err = types.Decode(version, &ver)
	if err != nil {
		storeLog.Info("GetDbVersion", "types.Decode err", err)
		return 0
	}
	storeLog.Info("GetDbVersion", "blockchain db version", ver.Data)
	return ver.Data
}

func (bs *BlockStore) SetDbVersion(versionNo int64) error {
	ver := types.Int64{Data: versionNo}
	verByte := types.Encode(&ver)

	storeLog.Info("SetDbVersion", "blcokchain db version", versionNo)

	return bs.db.SetSync(version.BlockChainVerKey, verByte)
}

func (bs *BlockStore) GetUpgradeMeta() (*types.UpgradeMeta, error) {
	ver := types.UpgradeMeta{}
	version, err := bs.db.Get(version.LocalDBMeta)
	if err != nil && err != dbm.ErrNotFoundInDb {
		return nil, err
	}
	if len(version) == 0 {
		return &types.UpgradeMeta{Version: "0.0.0"}, nil
	}
	err = types.Decode(version, &ver)
	if err != nil {
		return nil, err
	}
	storeLog.Info("GetUpgradeMeta", "blockchain db version", ver)
	return &ver, nil
}

func (bs *BlockStore) SetUpgradeMeta(meta *types.UpgradeMeta) error {
	verByte := types.Encode(meta)
	storeLog.Info("SetUpgradeMeta", "meta", meta)
	return bs.db.SetSync(version.LocalDBMeta, verByte)
}

func (bs *BlockStore) GetStoreUpgradeMeta() (*types.UpgradeMeta, error) {
	ver := types.UpgradeMeta{}
	version, err := bs.db.Get(version.StoreDBMeta)
	if err != nil && err != dbm.ErrNotFoundInDb {
		return nil, err
	}
	if len(version) == 0 {
		return &types.UpgradeMeta{Version: "0.0.0"}, nil
	}
	err = types.Decode(version, &ver)
	if err != nil {
		return nil, err
	}
	storeLog.Info("GetStoreUpgradeMeta", "blockchain db version", ver)
	return &ver, nil
}

func (bs *BlockStore) SetStoreUpgradeMeta(meta *types.UpgradeMeta) error {
	verByte := types.Encode(meta)
	storeLog.Debug("SetStoreUpgradeMeta", "meta", meta)
	return bs.db.SetSync(version.StoreDBMeta, verByte)
}

const (
	seqStatusOk = iota
	seqStatusNeedCreate
)

func (bs *BlockStore) CheckSequenceStatus(recordSequence bool) int {
	lastHeight := bs.Height()
	lastSequence, err := bs.LoadBlockLastSequence()
	if err != nil {
		if err != types.ErrHeightNotExist {
			storeLog.Error("CheckSequenceStatus", "LoadBlockLastSequence err", err)
			panic(err)
		}
	}
	if recordSequence {
		if lastSequence == -1 && lastHeight != -1 {
			storeLog.Error("CheckSequenceStatus", "lastHeight", lastHeight, "lastSequence", lastSequence)
			return seqStatusNeedCreate
		}
		if lastHeight > lastSequence {
			storeLog.Error("CheckSequenceStatus", "lastHeight", lastHeight, "lastSequence", lastSequence)
			return seqStatusNeedCreate
		}
		return seqStatusOk
	}
	if lastSequence != -1 {
		storeLog.Error("CheckSequenceStatus", "lastSequence", lastSequence)
		panic("can not disable isRecordBlockSequence")
	}
	return seqStatusOk
}

func (bs *BlockStore) CreateSequences(batchSize int64) {
	lastSeq, err := bs.LoadBlockLastSequence()
	if err != nil {
		if err != types.ErrHeightNotExist {
			storeLog.Error("CreateSequences LoadBlockLastSequence", "error", err)
			panic("CreateSequences LoadBlockLastSequence" + err.Error())
		}
	}
	storeLog.Info("CreateSequences LoadBlockLastSequence", "start", lastSeq)

	newBatch := bs.NewBatch(true)
	lastHeight := bs.Height()

	for i := lastSeq + 1; i <= lastHeight; i++ {
		seq := i
		header, err := bs.GetBlockHeaderByHeight(i)
		if err != nil {
			storeLog.Error("CreateSequences GetBlockHeaderByHeight", "height", i, "error", err)
			panic("CreateSequences GetBlockHeaderByHeight" + err.Error())
		}

		var blockSequence types.BlockSequence
		blockSequence.Hash = header.Hash
		blockSequence.Type = types.AddBlock
		BlockSequenceByte, err := proto.Marshal(&blockSequence)
		if err != nil {
			storeLog.Error("CreateSequences Marshal BlockSequence", "height", i, "hash", common.ToHex(header.Hash), "error", err)
			panic("CreateSequences Marshal BlockSequence" + err.Error())
		}
		newBatch.Set(calcSequenceToHashKey(seq, bs.isParaChain), BlockSequenceByte)

		sequenceBytes := types.Encode(&types.Int64{Data: seq})
		newBatch.Set(calcHashToSequenceKey(header.Hash, bs.isParaChain), sequenceBytes)

		if i-lastSeq == batchSize {
			storeLog.Info("CreateSequences ", "height", i)
			newBatch.Set(calcLastSeqKey(bs.isParaChain), types.Encode(&types.Int64{Data: i}))
			err = newBatch.Write()
			if err != nil {
				storeLog.Error("CreateSequences newBatch.Write", "error", err)
				panic("CreateSequences newBatch.Write" + err.Error())
			}
			lastSeq = i
			newBatch.Reset()
		}
	}
	newBatch.Set(calcLastSeqKey(bs.isParaChain), types.Encode(&types.Int64{Data: lastHeight}))
	err = newBatch.Write()
	if err != nil {
		storeLog.Error("CreateSequences newBatch.Write", "error", err)
		panic("CreateSequences newBatch.Write" + err.Error())
	}
	storeLog.Info("CreateSequences done")
}

func (bs *BlockStore) SetConsensusPara(kvs *types.LocalDBSet) error {
	var isSync bool
	if kvs.GetTxid() != 0 {
		isSync = true
	}
	batch := bs.db.NewBatch(isSync)
	for i := 0; i < len(kvs.KV); i++ {
		if types.CheckConsensusParaTxsKey(kvs.KV[i].Key) {
			if kvs.KV[i].Value == nil {
				batch.Delete(kvs.KV[i].Key)
			} else {
				batch.Set(kvs.KV[i].Key, kvs.KV[i].Value)
			}
		} else {
			storeLog.Error("Set:CheckConsensusParaTxsKey:fail", "key", string(kvs.KV[i].Key))
		}
	}
	err := batch.Write()
	if err != nil {
		panic(err)
	}
	return err
}

func (bs *BlockStore) saveBlockForTable(storeBatch dbm.Batch, blockdetail *types.BlockDetail, isBestChain, isSaveReceipt bool) error {

	height := blockdetail.Block.Height
	cfg := bs.client.GetConfig()
	hash := blockdetail.Block.Hash(cfg)

	blockbody := bs.BlockdetailToBlockBody(blockdetail)
	blockbody.Receipts = reduceReceipts(blockbody)
	bodykvs, err := saveBlockBodyTable(bs.db, blockbody)
	if err != nil {
		storeLog.Error("SaveBlockForTable:saveBlockBodyTable", "height", height, "hash", common.ToHex(hash), "err", err)
		return err
	}
	for _, kv := range bodykvs {
		storeBatch.Set(kv.GetKey(), kv.GetValue())
	}

	if isSaveReceipt {
		blockReceipt := &types.BlockReceipt{
			Receipts: blockdetail.Receipts,
			Hash:     hash,
			Height:   height,
		}
		recptkvs, err := saveBlockReceiptTable(bs.db, blockReceipt)
		if err != nil {
			storeLog.Error("SaveBlockForTable:saveBlockReceiptTable", "height", height, "hash", common.ToHex(hash), "err", err)
			return err
		}
		for _, kv := range recptkvs {
			storeBatch.Set(kv.GetKey(), kv.GetValue())
		}
	}

	var blockheader types.Header
	blockheader.Version = blockdetail.Block.Version
	blockheader.ParentHash = blockdetail.Block.ParentHash
	blockheader.TxHash = blockdetail.Block.TxHash
	blockheader.StateHash = blockdetail.Block.StateHash
	blockheader.Height = blockdetail.Block.Height
	blockheader.BlockTime = blockdetail.Block.BlockTime
	blockheader.Signature = blockdetail.Block.Signature
	blockheader.Difficulty = blockdetail.Block.Difficulty
	blockheader.Hash = hash
	blockheader.TxCount = int64(len(blockdetail.Block.Txs))

	headerkvs, err := saveHeaderTable(bs.db, &blockheader)
	if err != nil {
		storeLog.Error("SaveBlock:saveHeaderTable", "height", height, "hash", common.ToHex(hash), "err", err)
		return err
	}
	for _, kv := range headerkvs {
		storeBatch.Set(kv.GetKey(), kv.GetValue())
	}

	if isBestChain && !bs.isParaChain {
		paratxkvs, err := saveParaTxTable(cfg, bs.db, height, hash, blockdetail.Block.Txs)
		if err != nil {
			storeLog.Error("SaveBlock:saveParaTxTable", "height", height, "hash", common.ToHex(hash), "err", err)
			return err
		}
		for _, kv := range paratxkvs {
			storeBatch.Set(kv.GetKey(), kv.GetValue())
		}
	}
	return nil
}

func (bs *BlockStore) loadBlockByIndex(indexName string, prefix []byte, primaryKey []byte) (*types.BlockDetail, error) {
	cfg := bs.client.GetConfig()
	blockheader, err := getHeaderByIndex(bs.db, indexName, prefix, primaryKey)
	if blockheader == nil || err != nil {
		if err != dbm.ErrNotFoundInDb {
			storeLog.Error("loadBlockByIndex:getHeaderByIndex", "indexName", indexName, "prefix", prefix, "primaryKey", primaryKey, "err", err)
		}
		return nil, types.ErrHashNotExist
	}

	blockbody, err := getBodyByIndex(bs.db, indexName, prefix, primaryKey)
	if blockbody == nil || err != nil {
		if err != dbm.ErrNotFoundInDb {
			storeLog.Error("loadBlockByIndex:getBodyByIndex", "indexName", indexName, "prefix", prefix, "primaryKey", primaryKey, "err", err)
		}
		return nil, types.ErrHashNotExist
	}

	blockreceipt := blockbody.Receipts
	if !cfg.IsEnable("reduceLocaldb") || bs.Height() < blockheader.Height+ReduceHeight {
		receipt, err := getReceiptByIndex(bs.db, indexName, prefix, primaryKey)
		if receipt != nil {
			blockreceipt = receipt.Receipts
		} else {
			storeLog.Error("loadBlockByIndex:getReceiptByIndex", "indexName", indexName, "prefix", prefix, "primaryKey", primaryKey, "err", err)
			if !cfg.IsEnable("reduceLocaldb") {
				return nil, types.ErrHashNotExist
			}
		}
	}

	var blockdetail types.BlockDetail
	var block types.Block

	block.Version = blockheader.Version
	block.ParentHash = blockheader.ParentHash
	block.TxHash = blockheader.TxHash
	block.StateHash = blockheader.StateHash
	block.Height = blockheader.Height
	block.BlockTime = blockheader.BlockTime
	block.Signature = blockheader.Signature
	block.Difficulty = blockheader.Difficulty
	block.Txs = blockbody.Txs
	block.MainHeight = blockbody.MainHeight
	block.MainHash = blockbody.MainHash

	blockdetail.Receipts = blockreceipt
	blockdetail.Block = &block
	return &blockdetail, nil
}

func (bs *BlockStore) loadHeaderByIndex(height int64) (*types.Header, error) {

	hash, err := bs.GetBlockHashByHeight(height)
	if err != nil {
		return nil, err
	}

	header, err := getHeaderByIndex(bs.db, "", calcHeightHashKey(height, hash), nil)
	if header == nil || err != nil {
		if err != dbm.ErrNotFoundInDb {
			storeLog.Error("loadHeaderByHeight:getHeaderByIndex", "error", err)
		}
		return nil, types.ErrHashNotExist
	}
	return header, nil
}

func (bs *BlockStore) loadBlockByHashOld(hash []byte) (*types.BlockDetail, error) {
	var blockdetail types.BlockDetail
	var blockheader types.Header
	var blockbody types.BlockBody
	var block types.Block

	header, err := bs.db.Get(calcHashToBlockHeaderKey(hash))
	if header == nil || err != nil {
		if err != dbm.ErrNotFoundInDb {
			storeLog.Error("loadBlockByHashOld:calcHashToBlockHeaderKey", "hash", common.ToHex(hash), "err", err)
		}
		return nil, types.ErrHashNotExist
	}
	err = proto.Unmarshal(header, &blockheader)
	if err != nil {
		storeLog.Error("loadBlockByHashOld", "err", err)
		return nil, err
	}

	body, err := bs.db.Get(calcHashToBlockBodyKey(hash))
	if body == nil || err != nil {
		if err != dbm.ErrNotFoundInDb {
			storeLog.Error("loadBlockByHashOld:calcHashToBlockBodyKey ", "err", err)
		}
		return nil, types.ErrHashNotExist
	}
	err = proto.Unmarshal(body, &blockbody)
	if err != nil {
		storeLog.Error("loadBlockByHashOld", "err", err)
		return nil, err
	}

	block.Version = blockheader.Version
	block.ParentHash = blockheader.ParentHash
	block.TxHash = blockheader.TxHash
	block.StateHash = blockheader.StateHash
	block.Height = blockheader.Height
	block.BlockTime = blockheader.BlockTime
	block.Signature = blockheader.Signature
	block.Difficulty = blockheader.Difficulty
	block.Txs = blockbody.Txs
	block.MainHeight = blockbody.MainHeight
	block.MainHash = blockbody.MainHash
	blockdetail.Receipts = blockbody.Receipts
	blockdetail.Block = &block

	return &blockdetail, nil
}

func (bs *BlockStore) loadBlockByHeightOld(height int64) (*types.BlockDetail, error) {
	hash, err := bs.GetBlockHashByHeight(height)
	if err != nil {
		return nil, err
	}
	return bs.loadBlockByHashOld(hash)
}

func (bs *BlockStore) getBlockHeaderByHeightOld(height int64) (*types.Header, error) {
	blockheader, err := bs.db.Get(calcHeightToBlockHeaderKey(height))
	if err != nil {
		var hash []byte
		hash, err = bs.GetBlockHashByHeight(height)
		if err != nil {
			return nil, err
		}
		blockheader, err = bs.db.Get(calcHashToBlockHeaderKey(hash))
	}
	if blockheader == nil || err != nil {
		if err != dbm.ErrNotFoundInDb {
			storeLog.Error("getBlockHeaderByHeightOld:calcHashToBlockHeaderKey", "error", err)
		}
		return nil, types.ErrHashNotExist
	}
	var header types.Header
	err = proto.Unmarshal(blockheader, &header)
	if err != nil {
		storeLog.Error("getBlockHeaderByHeightOld", "Could not unmarshal blockheader:", blockheader)
		return nil, err
	}
	return &header, nil
}

func (bs *BlockStore) loadBlockBySequenceOld(Sequence int64) (*types.BlockDetail, int64, error) {

	blockSeq, err := bs.GetBlockSequence(Sequence)
	if err != nil {
		return nil, 0, err
	}

	block, err := bs.loadBlockByHashOld(blockSeq.Hash)
	if err != nil {
		block, err = bs.LoadBlockByHash(blockSeq.Hash)
		if err != nil {
			storeLog.Error("getBlockHeaderByHeightOld", "Sequence", Sequence, "type", blockSeq.Type, "hash", common.ToHex(blockSeq.Hash), "err", err)
			panic(err)
		}
	}
	return block, blockSeq.GetType(), nil
}

func (bs *BlockStore) saveReduceLocaldbFlag() {
	kv := types.FlagKV(types.FlagReduceLocaldb, 1)
	err := bs.db.Set(kv.Key, kv.Value)
	if err != nil {
		panic(err)
	}
}
