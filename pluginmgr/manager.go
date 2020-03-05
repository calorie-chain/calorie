
package pluginmgr


var pluginItems = make(map[string]Plugin)

var once = &sync.Once{}

func InitExec(cfg *typ.CalorieConfig) {
	once.Do(func() {
		for _, item := range pluginItems {
			item.InitExec(cfg)
		}
	})
}

func GetExecList() (datas []string) {
	for _, plugin := range pluginItems {
		datas = append(datas, plugin.GetExecutorName())
	}
	return
}

func InitWallet(wallet wcom.WalletOperate, sub map[string][]byte) {
	once.Do(func() {
		for _, item := range pluginItems {
			item.InitWallet(wallet, sub)
		}
	})
}

func HasExec(name string) bool {
	for _, item := range pluginItems {
		if item.GetExecutorName() == name {
			return true
		}
	}
	return false
}

func Register(p Plugin) {
	if p == nil {
		panic("plugin param is nil")
	}
	packageName := p.GetName()
	if len(packageName) == 0 {
		panic("plugin package name is empty")
	}
	if _, ok := pluginItems[packageName]; ok {
		panic("execute plugin item is existed. name = " + packageName)
	}
	pluginItems[packageName] = p
}

func AddCmd(rootCmd *cobra.Command) {
	for _, item := range pluginItems {
		item.AddCmd(rootCmd)
	}
}

func AddRPC(s types.RPCServer) {
	for _, item := range pluginItems {
		item.AddRPC(s)
	}
}
