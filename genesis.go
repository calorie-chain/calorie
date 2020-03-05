
package account


func safeAdd(balance, amount int64) (int64, error) {
	if balance+amount < amount || balance+amount > types.MaxTokenBalance {
		return balance, types.ErrAmount
	}
	return balance + amount, nil
}

func (acc *DB) GenesisInit(addr string, amount int64) (receipt *types.Receipt, err error) {
	accTo := acc.LoadAccount(addr)
	copyto := *accTo
	accTo.Balance, err = safeAdd(accTo.GetBalance(), amount)
	if err != nil {
		return nil, err
	}
	receiptBalanceTo := &types.ReceiptAccountTransfer{
		Prev:    &copyto,
		Current: accTo,
	}
	acc.SaveAccount(accTo)
	receipt = acc.genesisReceipt(accTo, receiptBalanceTo)
	return receipt, nil
}

func (acc *DB) GenesisInitExec(addr string, amount int64, execaddr string) (receipt *types.Receipt, err error) {
	accTo := acc.LoadAccount(execaddr)
	copyto := *accTo
	accTo.Balance, err = safeAdd(accTo.GetBalance(), amount)
	if err != nil {
		return nil, err
	}
	receiptBalanceTo := &types.ReceiptAccountTransfer{
		Prev:    &copyto,
		Current: accTo,
	}
	acc.SaveAccount(accTo)
	receipt = acc.genesisReceipt(accTo, receiptBalanceTo)
	receipt2, err := acc.ExecDeposit(addr, execaddr, amount)
	if err != nil {
		panic(err)
	}
	ty := int32(types.TyLogGenesisDeposit)
	receipt2.Ty = ty
	receipt = acc.mergeReceipt(receipt, receipt2)
	return receipt, nil
}

func (acc *DB) genesisReceipt(accTo *types.Account, receiptTo proto.Message) *types.Receipt {
	ty := int32(types.TyLogGenesisTransfer)
	log2 := &types.ReceiptLog{
		Ty:  ty,
		Log: types.Encode(receiptTo),
	}
	kv := acc.GetKVSet(accTo)
	return &types.Receipt{
		Ty:   types.ExecOk,
		KV:   kv,
		Logs: []*types.ReceiptLog{log2},
	}
}