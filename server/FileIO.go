package main

import (
	"path/filepath"
	"io/ioutil"
	"path"
	pb "../protobuf/go"
	"os"
	"github.com/op/go-logging"
	"container/list"
	"bufio"
	"io"
	"github.com/golang/protobuf/jsonpb"
)

func saveBlock(js string) {
	_ = ioutil.WriteFile(filepath.Join(servermap[selfid-1].dataDir, GetHashString(js)+".block"), []byte(js), 0666)
	return
}

func loadBlocks() ([]string, error) {
	ans := make([]string, 0)
	dir_list, err := ioutil.ReadDir(servermap[selfid-1].dataDir)
	if err != nil {
		return []string{}, err
	}
	for _, v := range dir_list {
		if path.Ext(v.Name()) == ".block" {
			dat, err := ioutil.ReadFile(filepath.Join(servermap[selfid-1].dataDir, v.Name()))
			if err == nil {
				ans = append(ans, string(dat))
			}
		}
	}
	return ans, nil
}

func saveTransactions(txs []*pb.Transaction, log *logging.Logger) {
	file1, err := os.OpenFile(filepath.Join(servermap[selfid-1].dataDir, "transaction.data"), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		log.Warning("open transaction file error", err)
	} else {
		defer file1.Close()
		for _, tx := range txs {
			if js, err := pbMarshal.MarshalToString(tx); err == nil {
				file1.Write([]byte("\n"))
				file1.Write([]byte(js))
			} else {
				log.Debug("Save error:", err)
			}
		}
		file1.Write([]byte("\n"))
	}
}

func saveTxLoop(in chan *pb.Transaction, alarm Notification) {
	log := logging.MustGetLogger("savet")

	for {
		log.Debug("Transaction Saver Sleep")
		alarm.sleep()
		log.Debug("Transaction Saver Wake up")

		buffer := make([]*pb.Transaction, 0)
		loop := true
		for loop {
			select {
			case tmp := <-in:
				buffer = append(buffer, tmp)
			default:
				loop = false
			}
		}
		if len(buffer) > 0 {
			log.Debugf("Save %d transactions", len(buffer))
			saveTransactions(buffer, log)
		}

	}
}

func loadTransactions() *list.List {
	ans := list.New()
	file1, err := os.OpenFile(filepath.Join(servermap[selfid-1].dataDir, "transaction.data"), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Warning("open transaction file error", err)
	} else {
		defer file1.Close()
		br := bufio.NewReader(file1)
		for {
			line, err := br.ReadString('\n')
			if err == io.EOF {
				break
			} else if len(line) <= 1 {
				continue
			} else {
				js := line[:len(line)-1]
				//js = js[:len(js)-1]
				var tx pb.Transaction
				//err := json.Unmarshal(js, &tx)
				jsonpb.UnmarshalString(js, &tx)
				if err == nil {
					ans.PushBack(&tx)
				}
			}
		}
	}
	log.Noticef("Load %d transactions from disk", ans.Len())
	return ans
}
