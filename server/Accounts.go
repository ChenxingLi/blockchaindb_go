package main

import (
	"sync"
	"errors"
	pb "../protobuf/go"
	"container/list"
	"github.com/op/go-logging"
)

type AccountState struct {
	lock sync.RWMutex
	data map[string]int32
}

func (self *AccountState) add(key string, value int32) (int32, error) {
	self.lock.Lock()
	defer self.lock.Unlock()
	return self.add_nosync(key, value)
}

func (self *AccountState) add_nosync(key string, value int32) (int32, error) {
	_, success := self.data[key]
	if !success {
		self.data[key] = initBalance
	}
	if self.data[key]+value >= 0 {
		self.data[key] += value
		return self.data[key], nil
	} else {
		return 0, errors.New("Not enough money")
	}
}

func (self *AccountState) check_nosync(key string) int32 {
	value, success := self.data[key]
	if !success {
		return initBalance
	} else {
		return value
	}
}

func (self *AccountState) afford_nosync(key string, value int32) bool {
	_, success := self.data[key]
	if !success {
		return initBalance >= value
	} else {
		return self.data[key] >= value
	}
}

func (self *AccountState) filterTxArray(input []*pb.Transaction, limit int, log *logging.Logger) []*pb.Transaction {
	self.lock.RLock()

	tmpdata := AccountState{
		data: make(map[string]int32),
	}
	selfplag := make(map[string]bool)
	for key, value := range self.data {
		tmpdata.data[key] = value
	}
	self.lock.RUnlock()

	answer := make([]*pb.Transaction,0, int(limit/8))
	for _, tx := range input {
		//log.Debugf("From: %s, Value: %d, tx value: %d", tx.FromID, tmpdata.data[tx.FromID], tx.Value)
		if _, suc := selfplag[tx.UUID]; tmpdata.afford_nosync(tx.FromID, tx.Value) && !suc{
			//log.Debug("So, append")
			selfplag[tx.UUID] = true

			answer = append(answer, tx)
			if len(answer) == limit {
				return answer
			}
			tmpdata.add_nosync(tx.FromID, -tx.Value)
			tmpdata.add_nosync(tx.ToID, tx.Value - tx.MiningFee)
		}
	}
	//log.Debug(len(answer))

	return answer
}

func (self *AccountState) filterPendingTx() {

	self.lock.RLock()

	tmpdata := AccountState{
		data: make(map[string]int32),
	}
	for key, value := range self.data {
		tmpdata.data[key] = value
	}
	self.lock.RUnlock()

	var nextE *list.Element
	for e := pendingTxs.data.Front(); e != nil ; e = nextE {
		nextE = e.Next()
		tx , success := e.Value.(*pb.Transaction)
		if !success {
			panic("you code has bug")
		}

		if tmpdata.afford_nosync(tx.FromID, tx.Value){
			tmpdata.add_nosync(tx.FromID, -tx.Value)
			tmpdata.add_nosync(tx.ToID, tx.Value - tx.MiningFee)
		} else {
			pendingTxs.data.Remove(e)
			delete(pendingTxs.uuidmap, tx.UUID)
		}
	}
}