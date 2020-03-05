
package pluginmgr


type Plugin interface {
	GetName() string
	GetExecutorName() string
	InitExec(cfg *typ.CalorieConfig)
	InitWallet(wallet wcom.WalletOperate, sub map[string][]byte)
	AddCmd(rootCmd *cobra.Command)
	AddRPC(s types.RPCServer)
}
