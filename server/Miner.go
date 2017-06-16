package main

import (
	"container/list"
	"sync"
	pb "../protobuf/go"
	"sort"
	"fmt"
	"encoding/json"
	"github.com/op/go-logging"
	mysha2 "../sha256-simd-master"
)

type Miner struct {
	lock      sync.RWMutex
	longest   *RichBlock
	miningTxs TxList
	uuidmap   map[string]bool

	alarm Notification
}

type TxList []*pb.Transaction

func newMiner() *Miner {
	var miner Miner
	miner.longest = blockchain.longest
	miner.miningTxs = TxList{}
	miner.uuidmap = make(map[string]bool)

	miner.alarm.channel = make(chan int, 1)
	return &miner
}

func (self *Miner) insertTxs(newTxs *list.List, log *logging.Logger) {
	for e := newTxs.Front(); e != nil; e = e.Next() {
		tx := e.Value.(*pb.Transaction)
		if _, exist := self.uuidmap[tx.UUID]; exist {
			//log.Debugf("[    ]Transaction exist")
			continue
		}
		self.miningTxs = append(self.miningTxs, tx)
	}

	sort.Sort(self.miningTxs)
	log.Debugf("[    ]Transaction before filter: %d", len(self.miningTxs))
	self.miningTxs = self.longest.accounts.filterTxArray(self.miningTxs, blockLimit, log)
	log.Debugf("[    ]Transaction after  filter: %d", len(self.miningTxs))

	self.alarm.wake() //do some notify things
}

func (self *Miner) update(log *logging.Logger) bool {

	log.Debug("[wait]Blockchain read Lock1")
	blockchain.lock1.RLock()
	log.Debug("[lock]Blockchain read Lock1")
	log.Debugf("[    ]Self longest ID %d, blockchain longest id %d", self.longest.block.BlockID, blockchain.longest.block.BlockID)

	if self.longest == blockchain.longest {
		log.Debug("[rels]Blockchain read Lock1")
		blockchain.lock1.RUnlock()
		return false
	}

	// Update UUIDmap
	tmpBlock1 := self.longest
	for !blockchain.checkAncestor_nosync(blockchain.longest, tmpBlock1, log) {
		self.undoUUIDmap(tmpBlock1)
		tmpBlock1 = blockchain.blocks[tmpBlock1.block.PrevHash]
	}
	tmpBlock2 := blockchain.longest
	for tmpBlock1 != tmpBlock2 {
		self.doUUIDmap(tmpBlock2)
		tmpBlock2 = blockchain.blocks[tmpBlock2.block.PrevHash]
	}

	self.longest = blockchain.longest

	blockchain.lock1.RUnlock()
	log.Debug("[rels]Blockchain read Lock1")

	// Change Pending
	log.Debug("[wait]Tx data Lock")

	pendingTxs.dataLock.Lock()
	log.Debug("[lock]Tx data Lock")

	log.Debugf("[    ]Pending list before deduplicate: %d", pendingTxs.data.Len())

	var eNext *list.Element
	for e := pendingTxs.data.Front(); e != nil; e = eNext {
		eNext = e.Next()
		tx, success := e.Value.(*pb.Transaction)
		if !success {
			log.Error("Invalid format in list")
		}
		if _, exist := self.uuidmap[tx.UUID]; exist {
			pendingTxs.data.Remove(e)
			delete(pendingTxs.uuidmap, tx.UUID)
		}
	}

	log.Debugf("[    ]Pending list before filter: %d", pendingTxs.data.Len())
	if pendingTxs.data.Len() > pendingFilterThreshold {
		log.Debugf("[    ]Pending list before filter: %d", pendingTxs.data.Len())
		self.longest.accounts.filterPendingTx()
		log.Debugf("[    ]Pending list after  filter: %d", pendingTxs.data.Len())
	} else {
		log.Debugf("[    ]Pending list length: %d, do not need filter", pendingTxs.data.Len())
	}

	self.miningTxs = make([]*pb.Transaction, 0, blockLimit/8)
	self.insertTxs(pendingTxs.data, log) //Rebuild miningTx using pending
	pendingTxs.dataLock.Unlock()
	log.Debug("[rels]Tx data Lock")

	return true
}

func (self *Miner) doUUIDmap(block *RichBlock) {
	for _, tx := range block.block.Transactions {
		self.uuidmap[tx.UUID] = true
	}
}

func (self *Miner) undoUUIDmap(block *RichBlock) {
	for _, tx := range block.block.Transactions {
		delete(self.uuidmap, tx.UUID)
	}
}

func (self *Miner) run_producer() {
	log := logging.MustGetLogger("miner")
	inchannel := make([]chan string, minerThreads)
	alarms := make([]Notification, minerThreads)
	outchannel := make(chan string)
	for i := 0; i < minerThreads; i++ {
		inchannel[i] = make(chan string, 100)
		alarms[i].channel = make(chan int, 1)
		go mine(inchannel[i], outchannel, alarms[i], i)
	}
	go self.run_minter(outchannel)

	blockchain.lock1.RLock()
	self.longest = blockchain.blocks[zeroHash]
	blockchain.lock1.RUnlock()

	log.Info("Miner Prepared")

	for {
		log.Debug("Miner Sleep")
		self.alarm.sleep()
		log.Debug("Miner Wake up")

		self.lock.Lock()
		log.Debug("[lock]Miner lock")
		self.update(log)
		if len(self.miningTxs) > 0 {
			block := pb.Block{
				BlockID:      self.longest.block.BlockID + 1,
				PrevHash:     self.longest.hash,
				Transactions: self.miningTxs,
				MinerID:      fmt.Sprintf("Server%02d", selfid),
				Nonce:        "00000000",
			}
			jsonstring, err := json.Marshal(block)
			if err != nil {
				log.Infof("json encoding error: %v", err)
			}
			in := string(jsonstring[:len(jsonstring)-10])
			log.Debugf("[    ]Update json")
			for i := 0; i < minerThreads; i++ {
				inchannel[i] <- in
				alarms[i].wake()
			}
		} else {
			log.Debugf("[    ]Block Slave")
			for i := 0; i < minerThreads; i++ {
				inchannel[i] <- ""
			}
		}

		self.lock.Unlock()
		log.Debug("[rels]Miner lock")

	}
}

func (self *Miner) run_minter(out chan string) {
	log := logging.MustGetLogger("clect")
	for {
		js := <-out
		if CheckHash(GetHashString(js)) {
			log.Warningf("Mine: %s", js)
			pushBlocks(js)
			if block, err := blockchain.parse(&pb.JsonBlockString{js}); err == nil {
				blockchain.add(block, true, log)
			} else {
				log.Error("Self Block Wrong:", err)
			}
		} else if GetHashString(js) != GetFastHashString(js) {
			log.Error("Oracle is not sha256, change")
			sha256o = false
		} else {
			log.Errorf("Invalid hash: %s", GetHashString(js))
		}
	}
}

func mine(in chan string, out chan string, alarm Notification, i int) {
	log := logging.MustGetLogger(fmt.Sprintf("mine%01d", i))

	log.Info("Miner Slave start")

	currentJS := ""
	j := 0
	for {
		loop := true
		//log.Warning("tick")
		for loop {
			select {
			case tmp := <-in:
				currentJS = tmp
				//log.Debug("Js update: %.20s", currentJS)
			default:
				loop = false
			}
		}
		//log.Warning("tick")

		if currentJS == "" || len(currentJS) == 0{
			log.Debug("Miner Slave sleep")
			alarm.sleep()
			log.Debug("Miner Slave wake up")
		} else {
			sha2 := mysha2.New()
			sha2.Write([]byte(currentJS))
			for k := 0; k < 10000; k++ {
				var s string
				if sha256o {
					s = fmt.Sprintf("%x", sha2.Sum([]byte(fmt.Sprintf("%01d%03d%04d\"}", i, j, k))))
				} else {
					s = GetHashString(fmt.Sprintf("%s%01d%03d%04d\"}", currentJS, i, j, k))
				}
				if CheckHash(s) {
					answer := currentJS + fmt.Sprintf("%01d%03d%04d\"}", i, j, k)
					out <- answer
					currentJS = ""
					break
				}
			}
		}
		j++
		j = j % 1000
	}
}

func (self TxList) Len() int {
	return len(self)
}

func (self TxList) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}
func (self TxList) Less(i, j int) bool {
	if self[i].MiningFee != self[i].MiningFee {
		return self[i].MiningFee > self[j].MiningFee
	} else {
		return self[i].UUID > self[j].UUID
	}

}
