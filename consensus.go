
package consensus


func New(cfg *types.CalorieConfig) queue.Module {
	mcfg := cfg.GetModuleConfig().Consensus
	sub := cfg.GetSubConfig().Consensus
	con, err := consensus.Load(mcfg.Name)
	if err != nil {
		panic("Unsupported consensus type:" + mcfg.Name + " " + err.Error())
	}
	subcfg, ok := sub[mcfg.Name]
	if !ok {
		subcfg = nil
	}
	obj := con(mcfg, subcfg)
	consensus.QueryData.SetThis(mcfg.Name, reflect.ValueOf(obj))
	return obj
}
