package main

import (
	pb "../protobuf/go"
	"sync"
	"container/list"
	"fmt"
	"errors"
	"github.com/op/go-logging"
	"github.com/golang/protobuf/jsonpb"
)

const zeroHash = "0000000000000000000000000000000000000000000000000000000000000000"

// There should be one process can get lock1 write.

type BlockChain struct {
	lock1   sync.RWMutex          // Mutex for synchronize
	blocks  map[string]*RichBlock // Block Chain
	longest *RichBlock            // Longest block
	uuidmap map[string][]string

	lock2         sync.RWMutex
	pendingBlocks *list.List // Pending Blocks
	receivedMap   map[string]bool

	lock3     sync.RWMutex
	blacklist map[string]bool

	alarm Notification
}

type RichBlock struct {
	block     pb.Block
	hash      string
	json      string
	prevBlock []*RichBlock // Specified When add to blockchain
	accounts  AccountState
}

func newBlockChain() *BlockChain {
	var blockchain BlockChain

	genesisBlock := RichBlock{
		block: pb.Block{
			BlockID:      0,
			PrevHash:     "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
			Transactions: []*pb.Transaction{},
			MinerID:      "",
			Nonce:        "00000000",
		},
		hash:      zeroHash,
		json:      "",
		prevBlock: []*RichBlock{},
		accounts:  AccountState{},
	}
	genesisBlock.accounts.data = make(map[string]int32)

	blockchain.blocks = make(map[string]*RichBlock)
	blockchain.blocks[genesisBlock.hash] = &genesisBlock
	blockchain.longest = &genesisBlock
	blockchain.uuidmap = make(map[string][]string)

	blockchain.pendingBlocks = list.New()
	blockchain.receivedMap = make(map[string]bool)

	blockchain.blacklist = make(map[string]bool)

	blockchain.alarm.channel = make(chan int, 1)

	return &blockchain
}

func (self *BlockChain) prepare() {
	log.Info("Load from disk")
	seen := self.recoverFromDisk()

	log.Info("Load from network")
	self.recoverFromNetwork(seen)

	miner.alarm.wake()
}

func (self *BlockChain) recoverFromDisk() map[string]bool {
	seen := make(map[string]bool)
	seen[zeroHash] = true
	blocks, err := loadBlocks()
	if err != nil {
		log.Warningf("Load Blocks error: %v", err)
	} else {
		log.Debugf("Find %d Blocks", len(blocks))
		for _, js := range blocks {
			if block, err := blockchain.parse(&pb.JsonBlockString{js}); err == nil {
				seen[block.hash] = true
				blockchain.add(block, true, log)
			} else {
				//log.Debug("Disk file error: ", err)
			}
		}
	}
	return seen
}

func (self *BlockChain) recoverFromNetwork(seen map[string]bool) {
	go getHeightBlocks(seen)
}

func (self *BlockChain) parse(blockjson *pb.JsonBlockString) (*RichBlock, error) {
	jsonstring := blockjson.Json

	// Check hash
	hash := GetHashString(jsonstring)
	if !CheckHash(hash) {
		return nil, errors.New("Invalid hash")
	}

	// Get block
	block := pb.Block{}
	//err1 := json.Unmarshal([]byte(jsonstring), &block)
	err1 := jsonpb.UnmarshalString(jsonstring, &block)
	if err1 != nil {
		return nil, errors.New("Fail to Unmarshal")
	}

	// Check prevhash
	prevHash := block.PrevHash
	if !CheckHash(prevHash) {
		return nil, errors.New("Invalid Previous hash")
	}

	// Check miner_id
	var miner_id int
	_, err2 := fmt.Sscanf(block.MinerID, "Server%02d", &miner_id)
	if len(block.MinerID) != 8 || err2 != nil || !(miner_id >= 1 && miner_id <= len(servermap)) {
		return nil, errors.New("Wrong Server Id")
	}

	// Check Transaction Length
	if len(block.Transactions) > 50 {
		return nil, errors.New("Too Many Transactions")
	}

	// We don't check transactions here, because the father of this block may doesn't exist
	for _, transaction := range block.Transactions {
		if transaction.Value < transaction.MiningFee {
			return nil, errors.New("Not enough Mining fee")
		}
	}

	return &RichBlock{block: block, hash: hash, json: jsonstring}, nil

}

///Receive pending blocks
func (self *BlockChain) add(block *RichBlock, vip bool, log *logging.Logger) {
	self.lock2.Lock()
	defer self.lock2.Unlock()

	_, success := self.receivedMap[block.hash]
	log.Debugf("[         ]Add to pending blocks, height %d, %v", block.block.BlockID, !success)
	if !success {
		self.receivedMap[block.hash] = true
		if vip {
			self.pendingBlocks.PushFront(block)
		} else {
			self.pendingBlocks.PushBack(block)
		}
		self.alarm.wake() // Notify Pending Update
	}
}

func (self *BlockChain) insert(block *RichBlock, log *logging.Logger) (bool, error) {
	log.Debug("[wait]Blockchain lock1")
	self.lock1.Lock()
	log.Debug("[lock]Blockchain lock1")
	defer log.Debug("[rels]Blockchain lock1")
	defer self.lock1.Unlock()

	parent := self.blocks[block.block.PrevHash]
	if block.block.BlockID != parent.block.BlockID+1 { // Invalid Height
		return false, errors.New(fmt.Sprintf("Invalid block id, block is %d, but parent is %d", block.block.BlockID, parent.block.BlockID))
	}

	// Check Transactions
	block.accounts = AccountState{data: make(map[string]int32)}
	for k, v := range parent.accounts.data {
		block.accounts.data[k] = v
	}
	selfplag := make(map[string]bool)
	for _, transaction := range block.block.Transactions {
		id, hash := self.checkUuid_nosync(transaction.UUID, parent, log)
		if id > 0 {
			return false, errors.New(fmt.Sprintf("Invalid transaction uuid = %s, exists in previous block %s", transaction.UUID, hash))
		}

		_, err := block.accounts.add_nosync(transaction.FromID, - transaction.Value)
		if err != nil {
			return false, errors.New(fmt.Sprintf("Account %s don't have enough money", transaction.FromID))
		}
		if _, suc := selfplag[transaction.UUID]; suc {
			//log.Error("self plag")
			return false, errors.New(fmt.Sprintf("self plagiarism tx, %v", transaction))
		}

		block.accounts.add_nosync(transaction.ToID, transaction.Value-transaction.MiningFee)
		block.accounts.add_nosync(block.block.MinerID, transaction.MiningFee)
		selfplag[transaction.UUID] = true
	}

	//Build block history index
	block.prevBlock = make([]*RichBlock, 0, 5)
	index := 0
	s := ""
	for tmp := int32(1); tmp < block.block.BlockID; tmp *= 2 {
		if parent.block.BlockID%tmp == 0 {
			block.prevBlock = append(block.prevBlock, parent)
			s += fmt.Sprintf("%d, ", parent.block.BlockID)
		} else {
			block.prevBlock = append(block.prevBlock, parent.prevBlock[index])
			s += fmt.Sprintf("%d, ", parent.prevBlock[index].block.BlockID)
		}
		index ++
	}

	//Insert block
	self.blocks[block.hash] = block

	//Update UUID map
	for _, transaction := range block.block.Transactions {
		_, success := self.uuidmap[transaction.UUID]
		if !success {
			self.uuidmap[transaction.UUID] = []string{block.hash}
		} else {
			self.uuidmap[transaction.UUID] = append(self.uuidmap[transaction.UUID], block.hash)
		}
	}

	//Update longest
	update := false
	if self.longest.block.BlockID < block.block.BlockID {
		self.longest = block
		update = true
	}

	log.Noticef("Insert: \n BlockID: %d, MinerID: %s, hash: %s, \n %v, %v", block.block.BlockID, block.block.MinerID, block.hash, block.json, block.accounts)
	log.Warningf("Insert:  BlockID: %d, MinerID: %s, hash: %10.10s, %s", block.block.BlockID, block.block.MinerID, block.hash, s)
	go saveBlock(block.json)

	return update, nil
}

func (self *BlockChain) checkUuid_nosync(uuid string, block *RichBlock, log *logging.Logger) (int32, string) {
	hashes, success := self.uuidmap[uuid]

	if success {
		for _, hash := range hashes {
			ancestor, suc := self.blocks[hash]
			if !suc {
				log.Error("You code have bug")
			}
			if self.checkAncestor_nosync(block, ancestor, log) {
				return ancestor.block.BlockID, ancestor.hash
			}
		}
	}
	return 0, ""
}

func (self *BlockChain) checkAncestor_nosync(block *RichBlock, ancestor *RichBlock, log *logging.Logger) bool {
	if ancestor.hash == zeroHash {
		return true
	}

	if block.block.BlockID <= ancestor.block.BlockID {
		return block.hash == ancestor.hash
	}

	aimBlockID := ancestor.block.BlockID
	currentBlock := block
	for {
		if currentBlock.block.BlockID <= ancestor.block.BlockID {
			log.Error("Previous cursor wrong")
		}
		for _, tmpBlock := range currentBlock.prevBlock {
			if tmpBlock.block.BlockID == aimBlockID {
				return tmpBlock.hash == ancestor.hash
			} else if tmpBlock.block.BlockID < aimBlockID {
				break
			}
			currentBlock = tmpBlock
		}
	}

}

///Push pending Blocks to blocks
func (self *BlockChain) run() {
	log := logging.MustGetLogger("push")

	log.Info("Blockchain Push Start")
	workingBlocks := list.New()
	statusList := make(map[string]int8)

	var update bool

	for {
		log.Debug("Block Pusher Sleep:", )
		self.alarm.sleep() // block this thread
		log.Debug("Block Pusher Wake up")

		log.Debug("[wait]Blockchain lock2")
		self.lock2.Lock()
		log.Debug("[lock]Blockchain lock2")
		workingBlocks.PushBackList(self.pendingBlocks)
		self.pendingBlocks = list.New()
		self.lock2.Unlock()
		log.Debug("[rels]Blockchain lock2")

		update = false
		var nextE *list.Element
		for e := workingBlocks.Front(); e != nil; e = nextE {
			nextE = e.Next()

			block := e.Value.(*RichBlock)

			currentStatus := statusList[block.hash]
			//log.Debugf("[status]StartCur: %s,%d", block.hash, statusList[block.hash])

			if currentStatus == 0 {
				log.Debug("[wait]Blockchain read lock1")
				self.lock1.RLock()
				log.Debug("[lock]Blockchain read lock1")
				_, suc_current := self.blocks[block.hash]
				self.lock1.RUnlock()
				log.Debug("[rels]Blockchain read lock1")

				if suc_current {
					statusList[block.hash] = 1
				} else {
					statusList[block.hash] = 2

				}
				currentStatus = statusList[block.hash]
			}

			switch {
			case currentStatus == -1 || currentStatus == 1:
				workingBlocks.Remove(e)
				continue
			case currentStatus >= 2:
				statusList[block.hash] = 2
			}

			prevHash := block.block.PrevHash
			prevStatus := statusList[prevHash]
			if prevStatus == 0 {
				log.Debug("[wait]Blockchain read lock1")
				self.lock1.RLock()
				log.Debug("[lock]Blockchain read lock1")
				_, suc_prev := self.blocks[prevHash]
				self.lock1.RUnlock()
				log.Debug("[rels]Blockchain read lock1")

				if suc_prev {
					statusList[prevHash] = 1
				} else {
					statusList[prevHash] = 4
				}
				prevStatus = statusList[prevHash]
			}

			switch prevStatus {
			case -1:
				statusList[block.hash] = -1
				self.alarm.wake()
				workingBlocks.Remove(e)
			case 1:
				tmpUpdate, err := self.insert(block, log)
				log.Debug("insert return")
				if err == nil {
					statusList[block.hash] = 1
					self.alarm.wake()
					update = update || tmpUpdate
				} else {
					log.Info("Invalid block", err)
					statusList[block.hash] = -1
					self.alarm.wake()
				}
				workingBlocks.Remove(e)
			case 3:
				log.Debug("[wait]Blockchain read lock3")
				self.lock3.RLock()
				log.Debug("[lock]Blockchain read lock3")
				_, black := self.blacklist[prevHash]
				self.lock3.RUnlock()
				log.Debug("[rels]Blockchain read lock3")

				if black {
					statusList[prevHash] = -1
					statusList[block.hash] = -1
					self.alarm.wake()
					workingBlocks.Remove(e)
				}
			default:
				// Wait for next turn
			}
			//log.Debugf("[status]Cur: %s,%d", block.hash, statusList[block.hash])
			//log.Debugf("[status]Pre: %s,%d", prevHash, statusList[prevHash])
		}

		if update {
			miner.alarm.wake() // Notify Miner
		}

		hashes := make([]string, 0)
		for key, value := range statusList {
			if value == 4 {
				hashes = append(hashes, key)
				statusList[key] = 3
			}
		}
		if len(hashes) > 0 {
			go queryBlocks(hashes) // Start query
		}
	}
}
