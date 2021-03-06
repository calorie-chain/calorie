package util



func init() {
	rand.Seed(types.Now().UnixNano())
}

var ulog = log15.New("module", "util")

func GetParaExecName(paraName string, name string) string {
	if strings.HasPrefix(name, "user.p.") {
		return name
	}
	return paraName + name
}

func MakeStringToUpper(in string, pos, count int) (out string, err error) {
	l := len(in)
	if pos < 0 || pos >= l || (pos+count) >= l || count <= 0 {
		err = fmt.Errorf("Invalid params. in=%s pos=%d count=%d", in, pos, count)
		return
	}
	tmp := []rune(in)
	for n := pos; n < pos+count; n++ {
		tmp[n] = unicode.ToUpper(tmp[n])
	}
	out = string(tmp)
	return
}

func MakeStringToLower(in string, pos, count int) (out string, err error) {
	l := len(in)
	if pos < 0 || pos >= l || (pos+count) >= l || count <= 0 {
		err = fmt.Errorf("Invalid params. in=%s pos=%d count=%d", in, pos, count)
		return
	}
	tmp := []rune(in)
	for n := pos; n < pos+count; n++ {
		tmp[n] = unicode.ToLower(tmp[n])
	}
	out = string(tmp)
	return
}

func GenNoneTxs(cfg *types.CalorieConfig, priv crypto.PrivKey, n int64) (txs []*types.Transaction) {
	for i := 0; i < int(n); i++ {
		txs = append(txs, CreateNoneTx(cfg, priv))
	}
	return txs
}

func GenCoinsTxs(cfg *types.CalorieConfig, priv crypto.PrivKey, n int64) (txs []*types.Transaction) {
	to, _ := Genaddress()
	for i := 0; i < int(n); i++ {
		txs = append(txs, CreateCoinsTx(cfg, priv, to, n+1))
	}
	return txs
}

func Genaddress() (string, crypto.PrivKey) {
	cr, err := crypto.New(types.GetSignName("", types.SECP256K1))
	if err != nil {
		panic(err)
	}
	privto, err := cr.GenKey()
	if err != nil {
		panic(err)
	}
	addrto := address.PubKeyToAddress(privto.PubKey().Bytes())
	return addrto.String(), privto
}

func CreateNoneTx(cfg *types.CalorieConfig, priv crypto.PrivKey) *types.Transaction {
	return CreateTxWithExecer(cfg, priv, "none")
}

func CreateTxWithExecer(cfg *types.CalorieConfig, priv crypto.PrivKey, execer string) *types.Transaction {
	if execer == "coins" {
		to, _ := Genaddress()
		return CreateCoinsTx(cfg, priv, to, types.Coin)
	}
	tx := &types.Transaction{Execer: []byte(execer), Payload: []byte("none")}
	tx.To = address.ExecAddress(execer)
	tx, err := types.FormatTx(cfg, execer, tx)
	if err != nil {
		return nil
	}
	if priv != nil {
		tx.Sign(types.SECP256K1, priv)
	}
	return tx
}

type TestingT interface {
	Error(args ...interface{})
	Log(args ...interface{})
}

func JSONPrint(t TestingT, input interface{}) {
	data, err := json.MarshalIndent(input, "", "\t")
	if err != nil {
		t.Error(err)
		return
	}
	if t == nil {
		fmt.Println(string(data))
	} else {
		t.Log(string(data))
	}
}

func CreateManageTx(cfg *types.CalorieConfig, priv crypto.PrivKey, key, op, value string) *types.Transaction {
	v := &types.ModifyConfig{Key: key, Op: op, Value: value, Addr: ""}
	exec := types.LoadExecutorType("manage")
	if exec == nil {
		panic("manage exec is not init")
	}
	tx, err := exec.Create("Modify", v)
	if err != nil {
		panic(err)
	}
	tx, err = types.FormatTx(cfg, "manage", tx)
	if err != nil {
		return nil
	}
	tx.Sign(types.SECP256K1, priv)
	return tx
}

func CreateCoinsTx(cfg *types.CalorieConfig, priv crypto.PrivKey, to string, amount int64) *types.Transaction {
	tx := createCoinsTx(cfg, to, amount)
	tx.Sign(types.SECP256K1, priv)
	return tx
}

func createCoinsTx(cfg *types.CalorieConfig, to string, amount int64) *types.Transaction {
	exec := types.LoadExecutorType("coins")
	if exec == nil {
		panic("unknow driver coins")
	}
	tx, err := exec.AssertCreate(&types.CreateTx{
		To:     to,
		Amount: amount,
	})
	if err != nil {
		panic(err)
	}
	tx.To = to
	tx, err = types.FormatTx(cfg, "coins", tx)
	if err != nil {
		return nil
	}
	return tx
}

func CreateTxWithTxHeight(cfg *types.CalorieConfig, priv crypto.PrivKey, to string, amount, expire int64) *types.Transaction {
	tx := createCoinsTx(cfg, to, amount)
	tx.Expire = expire + types.TxHeightFlag
	tx.Sign(types.SECP256K1, priv)
	return tx
}

func GenTxsTxHeigt(cfg *types.CalorieConfig, priv crypto.PrivKey, n, height int64) (txs []*types.Transaction) {
	to, _ := Genaddress()
	for i := 0; i < int(n); i++ {
		tx := CreateTxWithTxHeight(cfg, priv, to, types.Coin*(n+1), 20+height)
		txs = append(txs, tx)
	}
	return txs
}

var zeroHash [32]byte

func CreateNoneBlock(cfg *types.CalorieConfig, priv crypto.PrivKey, n int64) *types.Block {
	newblock := &types.Block{}
	newblock.Height = 1
	newblock.BlockTime = types.Now().Unix()
	newblock.ParentHash = zeroHash[:]
	newblock.Txs = GenNoneTxs(cfg, priv, n)
	newblock.TxHash = merkle.CalcMerkleRoot(cfg, newblock.Height, newblock.Txs)
	return newblock
}

func CreateCoinsBlock(cfg *types.CalorieConfig, priv crypto.PrivKey, n int64) *types.Block {
	newblock := &types.Block{}
	newblock.Height = 1
	newblock.BlockTime = types.Now().Unix()
	newblock.ParentHash = zeroHash[:]
	newblock.Txs = GenCoinsTxs(cfg, priv, n)
	newblock.TxHash = merkle.CalcMerkleRoot(cfg, newblock.Height, newblock.Txs)
	return newblock
}

func ExecBlock(client queue.Client, prevStateRoot []byte, block *types.Block, errReturn, sync, checkblock bool) (*types.BlockDetail, []*types.Transaction, error) {
	ulog.Debug("ExecBlock", "height------->", block.Height, "ntx", len(block.Txs))
	beg := types.Now()
	defer func() {
		ulog.Info("ExecBlock", "height", block.Height, "ntx", len(block.Txs), "writebatchsync", sync, "cost", types.Since(beg))
	}()

	detail, deltx, err := PreExecBlock(client, prevStateRoot, block, errReturn, sync, checkblock)
	if err != nil {
		return nil, nil, err
	}
	err = ExecKVSetCommit(client, block.StateHash, false)
	if err != nil {
		return nil, nil, err
	}
	return detail, deltx, nil
}

func PreExecBlock(client queue.Client, prevStateRoot []byte, block *types.Block, errReturn, sync, checkblock bool) (*types.BlockDetail, []*types.Transaction, error) {
	beg := types.Now()
	if errReturn && block.Height > 0 && !block.CheckSign(client.GetConfig()) {
		return nil, nil, types.ErrSign
	}
	cacheTxs := types.TxsToCache(block.Txs)
	oldtxscount := len(cacheTxs)
	var err error
	cacheTxs, err = CheckTxDup(client, cacheTxs, block.Height)
	if err != nil {
		return nil, nil, err
	}
	ulog.Debug("PreExecBlock", "CheckTxDup", types.Since(beg))
	beg = types.Now()
	newtxscount := len(cacheTxs)
	if oldtxscount != newtxscount && errReturn {
		return nil, nil, types.ErrTxDup
	}
	ulog.Debug("PreExecBlock", "prevtx", oldtxscount, "newtx", newtxscount)
	block.Txs = types.CacheToTxs(cacheTxs)
	receipts, err := ExecTx(client, prevStateRoot, block)
	if err != nil {
		return nil, nil, err
	}
	ulog.Debug("PreExecBlock", "ExecTx", types.Since(beg))
	beg = types.Now()
	var kvset []*types.KeyValue
	var deltxlist = make(map[int]bool)
	var rdata []*types.ReceiptData 
	for i := 0; i < len(receipts.Receipts); i++ {
		receipt := receipts.Receipts[i]
		if receipt.Ty == types.ExecErr {
			ulog.Error("exec tx err", "err", receipt, "txhash", common.ToHex(block.Txs[i].Hash()))
			if errReturn { 
				return nil, nil, types.ErrBlockExec
			}
			deltxlist[i] = true
			continue
		}
		rdata = append(rdata, &types.ReceiptData{Ty: receipt.Ty, Logs: receipt.Logs})
		kvset = append(kvset, receipt.KV...)
	}
	kvset = DelDupKey(kvset)
	var deltx []*types.Transaction
	if len(deltxlist) > 0 {
		index := 0
		for i := 0; i < len(block.Txs); i++ {
			if deltxlist[i] {
				deltx = append(deltx, block.Txs[i])
				continue
			}
			block.Txs[index] = block.Txs[i]
			cacheTxs[index] = cacheTxs[i]
			index++
		}
		block.Txs = block.Txs[0:index]
		cacheTxs = cacheTxs[0:index]
	}
	if len(deltx) > 0 && errReturn {
		return nil, nil, types.ErrCheckTxHash
	}
	var calcHash []byte
	cfg := client.GetConfig()
	height := block.Height

	if cfg.IsPara() {
		height = block.MainHeight
	}
	if !cfg.IsFork(height, "ForkRootHash") {
		calcHash = merkle.CalcMerkleRootCache(cacheTxs)
	} else {
		temtxs := types.TransactionSort(block.Txs)
		calcHash = merkle.CalcMerkleRoot(cfg, height, temtxs)
	}
	if errReturn && !bytes.Equal(calcHash, block.TxHash) {
		return nil, nil, types.ErrCheckTxHash
	}
	ulog.Debug("PreExecBlock", "CalcMerkleRootCache", types.Since(beg))
	beg = types.Now()
	block.TxHash = calcHash
	var detail types.BlockDetail
	calcHash, err = ExecKVMemSet(client, prevStateRoot, block.Height, kvset, sync, false)
	if err != nil {
		return nil, nil, err
	}
	if errReturn && !bytes.Equal(block.StateHash, calcHash) {
		err = ExecKVSetRollback(client, calcHash)
		if err != nil {
			ulog.Error("PreExecBlock-->ExecKVSetRollback", "err", err)
		}
		if len(rdata) > 0 {
			for i, rd := range rdata {
				rd.OutputReceiptDetails(block.Txs[i].Execer, ulog)
			}
		}
		return nil, nil, types.ErrCheckStateHash
	}
	block.StateHash = calcHash
	detail.Block = block
	detail.Receipts = rdata
	if detail.Block.Height > 0 && checkblock {
		err := CheckBlock(client, &detail)
		if err != nil {
			ulog.Debug("CheckBlock-->", "err", err)
			return nil, nil, err
		}
	}
	ulog.Debug("PreExecBlock", "CheckBlock", types.Since(beg))

	detail.KV = kvset
	detail.PrevStatusHash = prevStateRoot
	return &detail, deltx, nil
}

func ExecBlockUpgrade(client queue.Client, prevStateRoot []byte, block *types.Block, sync bool) error {
	ulog.Debug("ExecBlockUpgrade", "height------->", block.Height, "ntx", len(block.Txs))
	beg := types.Now()
	beg1 := beg
	defer func() {
		ulog.Info("ExecBlockUpgrade", "height", block.Height, "ntx", len(block.Txs), "writebatchsync", sync, "cost", types.Since(beg1))
	}()

	var err error
	receipts, err := ExecTx(client, prevStateRoot, block)
	if err != nil {
		return err
	}
	ulog.Debug("ExecBlockUpgrade", "ExecTx", types.Since(beg))
	beg = types.Now()
	var kvset []*types.KeyValue
	for i := 0; i < len(receipts.Receipts); i++ {
		receipt := receipts.Receipts[i]
		kvset = append(kvset, receipt.KV...)
	}
	kvset = DelDupKey(kvset)
	calcHash, err := ExecKVMemSet(client, prevStateRoot, block.Height, kvset, sync, true)
	if err != nil {
		return err
	}
	if !bytes.Equal(block.StateHash, calcHash) {
		return types.ErrCheckStateHash
	}
	ulog.Debug("ExecBlockUpgrade", "CheckBlock", types.Since(beg))
	err = ExecKVSetCommit(client, calcHash, true)
	return err
}

func CreateNewBlock(cfg *types.CalorieConfig, parent *types.Block, txs []*types.Transaction) *types.Block {
	newblock := &types.Block{}
	newblock.Height = parent.Height + 1
	newblock.BlockTime = parent.BlockTime + 1
	newblock.ParentHash = parent.Hash(cfg)
	newblock.Txs = append(newblock.Txs, txs...)

	if cfg.IsFork(newblock.GetHeight(), "ForkRootHash") {
		newblock.Txs = types.TransactionSort(newblock.Txs)
	}
	newblock.TxHash = merkle.CalcMerkleRoot(cfg, newblock.Height, newblock.Txs)
	return newblock
}

func ExecAndCheckBlock(qclient queue.Client, block *types.Block, txs []*types.Transaction, result []int) (*types.Block, error) {
	return ExecAndCheckBlockCB(qclient, block, txs, func(index int, receipt *types.ReceiptData) error {
		if len(result) <= index {
			return errors.New("txs num and status len not equal")
		}
		status := result[index]
		if status == 0 && receipt != nil {
			return errors.New("must failed, but index = " + fmt.Sprint(index))
		}
		if status > 0 && receipt == nil {
			return errors.New("must not faild, but index = " + fmt.Sprint(index))
		}
		if status > 0 && receipt.Ty != int32(status) {
			return errors.New("status must equal, but index = " + fmt.Sprint(index))
		}
		return nil
	})
}

func ExecAndCheckBlockCB(qclient queue.Client, block *types.Block, txs []*types.Transaction, cb func(int, *types.ReceiptData) error) (*types.Block, error) {
	block2 := CreateNewBlock(qclient.GetConfig(), block, txs)
	detail, deltx, err := ExecBlock(qclient, block.StateHash, block2, false, true, false)
	if err != nil {
		return nil, err
	}
	for _, v := range deltx {
		s, err := types.PBToJSON(v)
		if err != nil {
			return nil, err
		}
		println(string(s))
	}
	var getIndex = func(hash []byte, txlist []*types.Transaction) int {
		for i := 0; i < len(txlist); i++ {
			if bytes.Equal(hash, txlist[i].Hash()) {
				return i
			}
		}
		return -1
	}
	for i := 0; i < len(txs); i++ {
		if getIndex(txs[i].Hash(), deltx) >= 0 {
			if err := cb(i, nil); err != nil {
				return nil, err
			}
		} else if index := getIndex(txs[i].Hash(), detail.Block.Txs); index >= 0 {
			if err := cb(i, detail.Receipts[index]); err != nil {
				return nil, err
			}
		}
	}
	return detail.Block, nil
}

func ResetDatadir(cfg *types.Config, datadir string) string {
	if len(datadir) >= 2 && datadir[:2] == "~/" {
		usr, err := user.Current()
		if err != nil {
			panic(err)
		}
		dir := usr.HomeDir
		datadir = filepath.Join(dir, datadir[2:])
	}
	if len(datadir) >= 6 && datadir[:6] == "$TEMP/" {
		dir, err := ioutil.TempDir("", "Caloriedatadir-")
		if err != nil {
			panic(err)
		}
		datadir = filepath.Join(dir, datadir[6:])
	}
	ulog.Info("current user data dir is ", "dir", datadir)
	cfg.Log.LogFile = filepath.Join(datadir, cfg.Log.LogFile)
	cfg.BlockChain.DbPath = filepath.Join(datadir, cfg.BlockChain.DbPath)
	cfg.P2P.DbPath = filepath.Join(datadir, cfg.P2P.DbPath)
	cfg.Wallet.DbPath = filepath.Join(datadir, cfg.Wallet.DbPath)
	cfg.Store.DbPath = filepath.Join(datadir, cfg.Store.DbPath)
	return datadir
}

func CreateTestDB() (string, db.DB, db.KVDB) {
	dir, err := ioutil.TempDir("", "goleveldb")
	if err != nil {
		panic(err)
	}
	leveldb, err := db.NewGoLevelDB("goleveldb", dir, 128)
	if err != nil {
		panic(err)
	}
	return dir, leveldb, db.NewKVDB(leveldb)
}

func CloseTestDB(dir string, dbm db.DB) {
	err := os.RemoveAll(dir)
	if err != nil {
		ulog.Info("RemoveAll ", "dir", dir, "err", err)
	}
	dbm.Close()
}

func SaveKVList(kvdb db.DB, kvs []*types.KeyValue) {
	batch := kvdb.NewBatch(true)
	for i := 0; i < len(kvs); i++ {
		if kvs[i].Value == nil {
			batch.Delete(kvs[i].Key)
			continue
		}
		batch.Set(kvs[i].Key, kvs[i].Value)
	}
	err := batch.Write()
	if err != nil {
		panic(err)
	}
}

func PrintKV(kvs []*types.KeyValue) {
	for i := 0; i < len(kvs); i++ {
		fmt.Printf("KV %d %s(%s)\n", i, string(kvs[i].Key), common.ToHex(kvs[i].Value))
	}
}

type MockModule struct {
	Key string
}

func (m *MockModule) SetQueueClient(client queue.Client) {
	go func() {
		client.Sub(m.Key)
		for msg := range client.Recv() {
			msg.Reply(client.NewMessage(m.Key, types.EventReply, &types.Reply{IsOk: false,
				Msg: []byte(fmt.Sprintf("mock %s module not handle message %v", m.Key, msg.Ty))}))
		}
	}()
}

func (m *MockModule) Wait() {}

func (m *MockModule) Close() {}
