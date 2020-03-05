package wallet


var (
	maxTxNumPerBlock int64 = types.MaxTxsPerBlock
	MaxTxHashsPerTime int64 = 100
	walletlog               = log.New("module", "wallet")
)

func init() {
	wcom.QueryData.Register("wallet", &Wallet{})
}

const (
	AddTx int32 = 20001
	DelTx int32 = 20002
	sendTx int32 = 30001
	recvTx int32 = 30002
)

type Wallet struct {
	client queue.Client
	api                client.QueueProtocolAPI
	mtx                sync.Mutex
	timeout            *time.Timer
	mineStatusReporter wcom.MineStatusReport
	isclosed           int32
	isWalletLocked     int32
	fatalFailureFlag   int32
	Password           string
	FeeAmount          int64
	EncryptFlag        int64
	wg                 *sync.WaitGroup
	walletStore        *walletStore
	random             *rand.Rand
	cfg                *types.Wallet
	done               chan struct{}
	rescanwg           *sync.WaitGroup
	lastHeader         *types.Header
	initFlag           uint32 	SignType    int
	minFee      int64
	accountdb   *account.DB
	accTokenMap map[string]*account.DB
}

func SetLogLevel(level string) {
	clog.SetLogLevel(level)
}

func DisableLog() {
	walletlog.SetHandler(log.DiscardHandler())
	storelog.SetHandler(log.DiscardHandler())
}

func New(cfg *types.CalorieConfig) *Wallet {
	mcfg := cfg.GetModuleConfig().Wallet
	walletStoreDB := dbm.NewDB("wallet", mcfg.Driver, mcfg.DbPath, mcfg.DbCache)
	walletStore := newStore(walletStoreDB)
	signType := types.GetSignType("", mcfg.SignType)
	if signType == types.Invalid {
		signType = types.SECP256K1
	}

	wallet := &Wallet{
		walletStore:      walletStore,
		isWalletLocked:   1,
		fatalFailureFlag: 0,
		wg:               &sync.WaitGroup{},
		FeeAmount:        walletStore.GetFeeAmount(mcfg.MinFee),
		EncryptFlag:      walletStore.GetEncryptionFlag(),
		done:             make(chan struct{}),
		cfg:              mcfg,
		rescanwg:         &sync.WaitGroup{},
		initFlag:         0,
		SignType:         signType,
		minFee:           mcfg.MinFee,
		accountdb:        account.NewCoinsAccount(cfg),
		accTokenMap:      make(map[string]*account.DB),
	}
	wallet.random = rand.New(rand.NewSource(types.Now().UnixNano()))
	wcom.QueryData.SetThis("wallet", reflect.ValueOf(wallet))
	return wallet
}

func (wallet *Wallet) Wait() {}

func (wallet *Wallet) RegisterMineStatusReporter(reporter wcom.MineStatusReport) error {
	if reporter == nil {
		return types.ErrInvalidParam
	}
	if wallet.mineStatusReporter != nil {
		return errors.New("ReporterIsExisted")
	}
	wallet.mineStatusReporter = reporter
	return nil
}

func (wallet *Wallet) GetConfig() *types.Wallet {
	return wallet.cfg
}

func (wallet *Wallet) GetAPI() client.QueueProtocolAPI {
	return wallet.api
}

func (wallet *Wallet) GetDBStore() dbm.DB {
	return wallet.walletStore.GetDB()
}

func (wallet *Wallet) GetSignType() int {
	return wallet.SignType
}

func (wallet *Wallet) GetPassword() string {
	wallet.mtx.Lock()
	defer wallet.mtx.Unlock()

	return wallet.Password
}

func (wallet *Wallet) Nonce() int64 {
	return wallet.random.Int63()
}

func (wallet *Wallet) AddWaitGroup(delta int) {
	wallet.wg.Add(delta)
}

func (wallet *Wallet) WaitGroupDone() {
	wallet.wg.Done()
}

func (wallet *Wallet) GetBlockHeight() int64 {
	return wallet.GetHeight()
}

func (wallet *Wallet) GetRandom() *rand.Rand {
	return wallet.random
}

func (wallet *Wallet) GetWalletDone() chan struct{} {
	return wallet.done
}

func (wallet *Wallet) GetLastHeader() *types.Header {
	wallet.mtx.Lock()
	defer wallet.mtx.Unlock()
	return wallet.lastHeader
}

func (wallet *Wallet) GetWaitGroup() *sync.WaitGroup {
	return wallet.wg
}

func (wallet *Wallet) GetAccountByLabel(label string) (*types.WalletAccountStore, error) {
	return wallet.walletStore.GetAccountByLabel(label)
}

func (wallet *Wallet) IsRescanUtxosFlagScaning() (bool, error) {
	in := &types.ReqNil{}
	flag := false
	for _, policy := range wcom.PolicyContainer {
		out, err := policy.Call("GetUTXOScaningFlag", in)
		if err != nil {
			if err.Error() == types.ErrNotSupport.Error() {
				continue
			}
			return flag, err
		}
		reply, ok := out.(*types.Reply)
		if !ok {
			err = types.ErrTypeAsset
			return flag, err
		}
		flag = reply.IsOk
		return flag, err
	}

	return flag, nil
}

func (wallet *Wallet) Close() {
	atomic.StoreInt32(&wallet.isclosed, 1)
	for _, policy := range wcom.PolicyContainer {
		policy.OnClose()
	}
	close(wallet.done)
	wallet.client.Close()
	wallet.wg.Wait()
	wallet.walletStore.Close()
	walletlog.Info("wallet module closed")
}

func (wallet *Wallet) IsClose() bool {
	return atomic.LoadInt32(&wallet.isclosed) == 1
}

func (wallet *Wallet) IsWalletLocked() bool {
	return atomic.LoadInt32(&wallet.isWalletLocked) != 0
}

func (wallet *Wallet) SetQueueClient(cli queue.Client) {
	var err error
	wallet.client = cli
	wallet.client.Sub("wallet")
	wallet.api, err = client.New(cli, nil)
	if err != nil {
		panic("SetQueueClient client.New err")
	}
	sub := cli.GetConfig().GetSubConfig().Wallet
	wcom.Init(wallet, sub)
	wallet.wg.Add(1)
	go wallet.ProcRecvMsg()
	for _, policy := range wcom.PolicyContainer {
		policy.OnSetQueueClient()
	}
	wallet.setInited(true)
}

func (wallet *Wallet) GetAccountByAddr(addr string) (*types.WalletAccountStore, error) {
	return wallet.walletStore.GetAccountByAddr(addr)
}

func (wallet *Wallet) SetWalletAccount(update bool, addr string, account *types.WalletAccountStore) error {
	return wallet.walletStore.SetWalletAccount(update, addr, account)
}

func (wallet *Wallet) GetPrivKeyByAddr(addr string) (crypto.PrivKey, error) {
	if !wallet.isInited() {
		return nil, types.ErrNotInited
	}
	wallet.mtx.Lock()
	defer wallet.mtx.Unlock()

	return wallet.getPrivKeyByAddr(addr)
}

func (wallet *Wallet) getPrivKeyByAddr(addr string) (crypto.PrivKey, error) {
	Accountstor, err := wallet.walletStore.GetAccountByAddr(addr)
	if err != nil {
		walletlog.Error("getPrivKeyByAddr", "GetAccountByAddr err:", err)
		return nil, err
	}

	prikeybyte, err := common.FromHex(Accountstor.GetPrivkey())
	if err != nil || len(prikeybyte) == 0 {
		walletlog.Error("getPrivKeyByAddr", "FromHex err", err)
		return nil, err
	}

	privkey := wcom.CBCDecrypterPrivkey([]byte(wallet.Password), prikeybyte)
	cr, err := crypto.New(types.GetSignName("", wallet.SignType))
	if err != nil {
		walletlog.Error("getPrivKeyByAddr", "err", err)
		return nil, err
	}
	priv, err := cr.PrivKeyFromBytes(privkey)
	if err != nil {
		walletlog.Error("getPrivKeyByAddr", "PrivKeyFromBytes err", err)
		return nil, err
	}
	return priv, nil
}

func (wallet *Wallet) AddrInWallet(addr string) bool {
	if !wallet.isInited() {
		return false
	}
	if len(addr) == 0 {
		return false
	}
	acc, err := wallet.walletStore.GetAccountByAddr(addr)
	if err == nil && acc != nil {
		return true
	}
	return false
}

func (wallet *Wallet) IsTransfer(addr string) (bool, error) {
	wallet.mtx.Lock()
	defer wallet.mtx.Unlock()

	return wallet.isTransfer(addr)
}

func (wallet *Wallet) isTransfer(addr string) (bool, error) {

	ok, err := wallet.checkWalletStatus()
	if ok || err == types.ErrSaveSeedFirst {
		return ok, err
	}
	if !wallet.isTicketLocked() {
		if addr == address.ExecAddress("ticket") {
			return true, nil
		}
	}
	return ok, err
}

func (wallet *Wallet) CheckWalletStatus() (bool, error) {
	if !wallet.isInited() {
		return false, types.ErrNotInited
	}

	wallet.mtx.Lock()
	defer wallet.mtx.Unlock()

	return wallet.checkWalletStatus()
}

func (wallet *Wallet) checkWalletStatus() (bool, error) {
	if wallet.IsWalletLocked() && !wallet.isTicketLocked() {
		return false, types.ErrOnlyTicketUnLocked
	} else if wallet.IsWalletLocked() {
		return false, types.ErrWalletIsLocked
	}

	has, err := wallet.walletStore.HasSeed()
	if !has || err != nil {
		return false, types.ErrSaveSeedFirst
	}
	return true, nil
}

func (wallet *Wallet) isTicketLocked() bool {
	locked := true
	if wallet.mineStatusReporter != nil {
		locked = wallet.mineStatusReporter.IsTicketLocked()
	}
	return locked
}

func (wallet *Wallet) isAutoMinning() bool {
	autoMining := false
	if wallet.mineStatusReporter != nil {
		autoMining = wallet.mineStatusReporter.IsAutoMining()
	}
	return autoMining
}

func (wallet *Wallet) GetWalletStatus() *types.WalletStatus {
	var err error
	s := &types.WalletStatus{}
	s.IsWalletLock = wallet.IsWalletLocked()
	s.IsHasSeed, err = wallet.walletStore.HasSeed()
	s.IsAutoMining = wallet.isAutoMinning()
	s.IsTicketLock = wallet.isTicketLocked()
	if err != nil {
		walletlog.Debug("GetWalletStatus HasSeed ", "err", err)
	}
	walletlog.Debug("GetWalletStatus", "walletstatus", s)
	return s
}


func (wallet *Wallet) getWalletAccounts() ([]*types.WalletAccountStore, error) {
	if !wallet.isInited() {
		return nil, types.ErrNotInited
	}

	WalletAccStores, err := wallet.walletStore.GetAccountByPrefix("Account")
	if err != nil || len(WalletAccStores) == 0 {
		walletlog.Info("GetWalletAccounts", "GetAccountByPrefix:err", err)
		return nil, err
	}
	return WalletAccStores, err
}

func (wallet *Wallet) GetWalletAccounts() ([]*types.WalletAccountStore, error) {
	if !wallet.isInited() {
		return nil, types.ErrNotInited
	}
	wallet.mtx.Lock()
	defer wallet.mtx.Unlock()

	return wallet.getWalletAccounts()
}

func (wallet *Wallet) updateLastHeader(block *types.BlockDetail, mode int) error {
	wallet.mtx.Lock()
	defer wallet.mtx.Unlock()
	header, err := wallet.api.GetLastHeader()
	if err != nil {
		return err
	}
	if block != nil {
		if mode == 1 && block.Block.Height > header.Height {
			wallet.lastHeader = &types.Header{
				BlockTime: block.Block.BlockTime,
				Height:    block.Block.Height,
				StateHash: block.Block.StateHash,
			}
		} else if mode == -1 && wallet.lastHeader != nil && wallet.lastHeader.Height == block.Block.Height {
			wallet.lastHeader = header
		}
	}
	if block == nil || wallet.lastHeader == nil {
		wallet.lastHeader = header
	}
	return nil
}

func (wallet *Wallet) setInited(flag bool) {
	if flag && !wallet.isInited() {
		atomic.StoreUint32(&wallet.initFlag, 1)
	}
}

func (wallet *Wallet) isInited() bool {
	return atomic.LoadUint32(&wallet.initFlag) != 0
}
