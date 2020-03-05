
package types


func (block *Block) Hash(cfg *CalorieConfig) []byte {
	if cfg.IsFork(block.Height, "ForkBlockHash") {
		return block.HashNew()
	}
	return block.HashOld()
}

func (block *Block) HashByForkHeight(forkheight int64) []byte {
	if block.Height >= forkheight {
		return block.HashNew()
	}
	return block.HashOld()
}

func (block *Block) HashNew() []byte {
	data, err := proto.Marshal(block.getHeaderHashNew())
	if err != nil {
		panic(err)
	}
	return common.Sha256(data)
}

func (block *Block) HashOld() []byte {
	data, err := proto.Marshal(block.getHeaderHashOld())
	if err != nil {
		panic(err)
	}
	return common.Sha256(data)
}

func (block *Block) Size() int {
	return Size(block)
}

func (block *Block) GetHeader(cfg *CalorieConfig) *Header {
	head := &Header{}
	head.Version = block.Version
	head.ParentHash = block.ParentHash
	head.TxHash = block.TxHash
	head.BlockTime = block.BlockTime
	head.Height = block.Height
	head.Difficulty = block.Difficulty
	head.StateHash = block.StateHash
	head.TxCount = int64(len(block.Txs))
	head.Hash = block.Hash(cfg)
	return head
}

func (block *Block) getHeaderHashOld() *Header {
	head := &Header{}
	head.Version = block.Version
	head.ParentHash = block.ParentHash
	head.TxHash = block.TxHash
	head.BlockTime = block.BlockTime
	head.Height = block.Height
	return head
}

func (block *Block) getHeaderHashNew() *Header {
	head := &Header{}
	head.Version = block.Version
	head.ParentHash = block.ParentHash
	head.TxHash = block.TxHash
	head.BlockTime = block.BlockTime
	head.Height = block.Height
	head.Difficulty = block.Difficulty
	head.StateHash = block.StateHash
	head.TxCount = int64(len(block.Txs))
	return head
}

func (block *Block) CheckSign(cfg *CalorieConfig) bool {
	if block.Signature != nil {
		hash := block.Hash(cfg)
		sign := block.GetSignature()
		if !CheckSign(hash, "", sign) {
			return false
		}
	}
	cpu := runtime.NumCPU()
	ok := checkAll(block.Txs, cpu)
	return ok
}

func gen(done <-chan struct{}, task []*Transaction) <-chan *Transaction {
	ch := make(chan *Transaction)
	go func() {
		defer func() {
			close(ch)
		}()
		for i := 0; i < len(task); i++ {
			select {
			case ch <- task[i]:
			case <-done:
				return
			}
		}
	}()
	return ch
}

type result struct {
	isok bool
}

func check(data *Transaction) bool {
	return data.CheckSign()
}

func checksign(done <-chan struct{}, taskes <-chan *Transaction, c chan<- result) {
	for task := range taskes {
		select {
		case c <- result{check(task)}:
		case <-done:
			return
		}
	}
}

func checkAll(task []*Transaction, n int) bool {
	done := make(chan struct{})
	defer close(done)

	taskes := gen(done, task)

	c := make(chan result) 
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			checksign(done, taskes, c) 
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(c) 
	}()
	for r := range c {
		if !r.isok {
			return false
		}
	}
	return true
}

func CheckSign(data []byte, execer string, sign *Signature) bool {
	c, err := crypto.New(GetSignName(execer, int(sign.Ty)))
	if err != nil {
		return false
	}
	pub, err := c.PubKeyFromBytes(sign.Pubkey)
	if err != nil {
		return false
	}
	signbytes, err := c.SignatureFromBytes(sign.Signature)
	if err != nil {
		return false
	}
	return pub.VerifyBytes(data, signbytes)
}

func (blockDetail *BlockDetail) FilterParaTxsByTitle(cfg *CalorieConfig, title string) *ParaTxDetail {
	var paraTx ParaTxDetail
	paraTx.Header = blockDetail.Block.GetHeader(cfg)

	for i := 0; i < len(blockDetail.Block.Txs); i++ {
		tx := blockDetail.Block.Txs[i]
		if IsSpecificParaExecName(title, string(tx.Execer)) {

			if tx.GroupCount >= 2 {
				txDetails, endIdx := blockDetail.filterParaTxGroup(tx, i)
				paraTx.TxDetails = append(paraTx.TxDetails, txDetails...)
				i = endIdx - 1
				continue
			}

			var txDetail TxDetail
			txDetail.Tx = tx
			txDetail.Receipt = blockDetail.Receipts[i]
			txDetail.Index = uint32(i)
			paraTx.TxDetails = append(paraTx.TxDetails, &txDetail)

		}
	}
	return &paraTx
}

func (blockDetail *BlockDetail) filterParaTxGroup(tx *Transaction, index int) ([]*TxDetail, int) {
	var headIdx int
	var txDetails []*TxDetail

	for i := index; i >= 0; i-- {
		if bytes.Equal(tx.Header, blockDetail.Block.Txs[i].Hash()) {
			headIdx = i
			break
		}
	}

	endIdx := headIdx + int(tx.GroupCount)
	for i := headIdx; i < endIdx; i++ {
		var txDetail TxDetail
		txDetail.Tx = blockDetail.Block.Txs[i]
		txDetail.Receipt = blockDetail.Receipts[i]
		txDetail.Index = uint32(i)
		txDetails = append(txDetails, &txDetail)
	}
	return txDetails, endIdx
}

func (blockDetail *BlockDetail) Size() int {
	return Size(blockDetail)
}

func (header *Header) Size() int {
	return Size(header)
}

func (paraTxDetail *ParaTxDetail) Size() int {
	return Size(paraTxDetail)
}
