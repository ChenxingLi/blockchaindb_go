package main

import (
	"sync"
	"container/list"
	pb "../protobuf/go"
	"github.com/op/go-logging"
)

type PendingTransactions struct {
	bufferLock    sync.RWMutex
	buffer        *list.List
	bufferuuidmap map[string]bool

	dataLock      sync.RWMutex
	data          *list.List
	uuidmap       map[string]bool

	alarm         Notification
	savechan chan *pb.Transaction
	saveAlarm Notification
}

func newPendingTransactions() *PendingTransactions{
	var pendingTxs PendingTransactions
	pendingTxs.buffer = list.New()
	pendingTxs.data = list.New()
	pendingTxs.bufferuuidmap = make(map[string]bool)
	pendingTxs.uuidmap = make(map[string]bool)
	pendingTxs.alarm.channel = make(chan int, 1)
	pendingTxs.savechan = make(chan *pb.Transaction, 10000)
	pendingTxs.saveAlarm.channel = make(chan int, 1)

	return &pendingTxs
}


func (self *PendingTransactions) prepare() {
	self.loadfromDisk()
	go saveTxLoop(self.savechan, self.saveAlarm)
}

func (self *PendingTransactions) loadfromDisk() {
	txs := loadTransactions()
	log.Info("Load Pending Tx")

	log.Debug("[wait]Tx data lock")
	self.dataLock.Lock()
	log.Debug("[lock]Tx data lock")

	self.data = txs
	for e:= self.data.Front(); e!=nil; e=e.Next() {
		tx := e.Value.(*pb.Transaction)
		self.uuidmap[tx.UUID] = true
	}
	self.dataLock.Unlock()
	log.Debug("[rels]Tx data lock")

}

func (self *PendingTransactions) addTx(tx *pb.Transaction, log *logging.Logger) bool {

	log.Debugf("Add Tx: %v",tx)

	self.bufferLock.RLock()
	_, success1 := self.bufferuuidmap[tx.UUID]
	self.bufferLock.RUnlock()

	self.dataLock.RLock()
	_, success2 := self.uuidmap[tx.UUID]
	self.dataLock.RUnlock()

	if !(success1 || success2) {
		self.bufferLock.Lock()
		self.bufferuuidmap[tx.UUID] = true
		self.buffer.PushBack(tx)
		self.bufferLock.Unlock()

		select {
		case self.savechan <- tx :
			self.saveAlarm.wake()
		default:
		}

		self.alarm.wake()
		return true
	} else {
		return false
	}
}

func (self *PendingTransactions) run() {
	log :=  logging.MustGetLogger("petx")
	log.Info("Pending Push Start")
	for {
		log.Debug("Pending Sleep")
		self.alarm.sleep()
		log.Debug("Pending Wake Up")

		log.Debug("[wait]Tx buffer lock")
		self.bufferLock.Lock()
		log.Debug("[lock]Tx buffer lock")
		tmpBuffer := self.buffer
		self.buffer = list.New()
		uuids := make([]string,0,len(self.uuidmap))
		for key, _ := range self.uuidmap {
			uuids = append(uuids, key)
		}
		self.uuidmap = make(map[string]bool)
		self.bufferLock.Unlock()
		log.Debug("[rels]Tx buffer lock")

		log.Debug("[wait]Tx data lock")
		self.dataLock.Lock()
		log.Debug("[lock]Tx data lock")
		self.data.PushBackList(tmpBuffer)
		for _, key := range uuids {
			self.uuidmap[key] = true
		}
		self.dataLock.Unlock()
		log.Debug("[rels]Tx data lock")


		log.Debug("[wait]Miner lock")
		miner.lock.Lock()
		log.Debug("[lock]Miner lock")
		miner.insertTxs(tmpBuffer, log)
		miner.lock.Unlock()
		log.Debug("[rels]Miner lock")

		//TODO: Log new data on server
	}
}