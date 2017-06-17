package main

import (
	"google.golang.org/grpc"
	pb "../protobuf/go"
	"fmt"
	"time"
	"errors"
	"golang.org/x/net/context"
	"github.com/op/go-logging"
	"math/rand"
)

type Server struct {
	id int
	ip, port, dataDir string
}

type RPCQuery struct {
	apiType uint8
	arg     interface{}
}

type RPCAnswer struct {
	apiType uint8
	ans     interface{}
	err     error
}

func newClient(server *Server) (pb.BlockChainMinerClient, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", server.ip, server.port), grpc.WithTimeout(time.Duration(timeoutSencond)*time.Second), grpc.WithInsecure())

	if err != nil {
		return nil, nil, err
	}
	//new client
	client := pb.NewBlockChainMinerClient(conn)
	return client, conn, err
}

func queryTask(c pb.BlockChainMinerClient, query RPCQuery, log *logging.Logger) RPCAnswer {
	switch query.apiType {
	case 4:
		argument, ok := query.arg.(*pb.Null)
		if !ok {
			return RPCAnswer{4, nil, errors.New("Invalid argument template")}
		}
		if r, err := c.GetHeight(context.Background(), argument); err != nil {
			return RPCAnswer{4, nil, err}
		} else {
			return RPCAnswer{4, r, nil}
		}
	case 5:
		argument, ok := query.arg.(*pb.GetBlockRequest)
		if !ok {
			return RPCAnswer{5, nil, errors.New("Invalid argument template")}
		}
		if r, err := c.GetBlock(context.Background(), argument); err != nil {
			return RPCAnswer{5, nil, err}
		} else {
			return RPCAnswer{5, r, nil}
		}
	case 6:
		argument, ok := query.arg.(*pb.JsonBlockString)
		if !ok {
			return RPCAnswer{6, nil, errors.New("Invalid argument template")}
		}
		if r, err := c.PushBlock(context.Background(), argument); err != nil {
			return RPCAnswer{6, nil, err}
		} else {
			return RPCAnswer{6, r, nil}
		}
	case 7:
		argument, ok := query.arg.(*pb.Transaction)
		if !ok {
			return RPCAnswer{7, nil, errors.New("Invalid argument template")}
		}
		if r, err := c.PushTransaction(context.Background(), argument); err != nil {
			return RPCAnswer{7, nil, err}
		} else {
			return RPCAnswer{7, r, nil}
		}
	}
	return RPCAnswer{0, nil, errors.New("Unserved")}
}

func queryTasks(server *Server, tasks []RPCQuery, log *logging.Logger) ([]*RPCAnswer, error) {
	client, conn, err := newClient(server)
	if err != nil {
		return nil, err
	} else {
		defer conn.Close()
	}
	ansset := make([]*RPCAnswer, 0, len(tasks))
	for _, task := range tasks {
		answer := queryTask(client, task, log)
		ansset = append(ansset, &answer)
	}
	return ansset, nil
}

func signalSlave(in chan *Server, tasks []RPCQuery, out chan []*RPCAnswer) {
	log := logging.MustGetLogger(fmt.Sprintf("s%04d",rand.Intn(1000)))
	for {
		server, ok := <-in
		if !ok {
			break
		}
		ansset, err := queryTasks(server, tasks, log)
		if err != nil {
			log.Warning(err)
			out <- nil
		} else {
			out <- ansset
		}
	}
}

func queryBlocks(hashes []string) {
	log := logging.MustGetLogger(fmt.Sprintf("gb%03d",rand.Intn(1000)))
	log.Infof("New manager for queryBlock: %v", hashes)


	manachan := make(chan *Server, nserver+1)
	anschan := make(chan []*RPCAnswer, nserver+1)

	blacklist := make(map[string]bool)
	tasks := make([]RPCQuery, 0, len(hashes))

	for _, hash := range hashes {
		blacklist[hash] = false
		tasks = append(tasks, RPCQuery{5, &pb.GetBlockRequest{hash}})
	}


	for i := 0; i < nserver; i++ {
		if i != selfid-1 {
			manachan <- &(servermap[i])
		}
	}
	close(manachan)

	for i := 0; i < queryBlockThreads; i++ {
		go signalSlave(manachan, tasks, anschan)
	}

	for i := 0; i < nserver -1; i++ {
		ansset := <-anschan
		log.Debug("One Client Response")
		for _, answer := range ansset {
			if answer == nil {
				log.Debug("Connection error")
				continue
			}
			if answer.err != nil {
				log.Debug(answer.err)
				continue
			}
			blockjson, ok := answer.ans.(*pb.JsonBlockString)
			if !ok {
				log.Debug("Answer format error")
				continue
			}
			if block, err := blockchain.parse(blockjson); err == nil {
				blockchain.add(block, true, log)
				delete(blacklist, block.hash)
			} else {
				log.Debug("Get wrong transaction", err)
			}
		}
		if len(blacklist) == 0 {
			break
		}
	}

	if len(blacklist) > 0 {
		blockchain.lock3.Lock()
		for key, _ := range blacklist {
			blockchain.blacklist[key] = true
		}
		blockchain.lock3.Unlock()
	}

	for {
		_, ok := <-manachan
		if !ok {
			break
		}
	}

	log.Debug("Done and wake up blockchain push")
	blockchain.alarm.wake()

}

func pushTransactions(transaction *pb.Transaction) bool {
	tasks := []RPCQuery{RPCQuery{apiType:7, arg: transaction}}
	manachan := make(chan *Server, nserver+1)
	anschan := make(chan []*RPCAnswer, nserver+1)

	for i := 0; i < nserver; i++ {
		if i != selfid-1 {
			manachan <- &(servermap[i])
		}
	}
	close(manachan)

	for i := 0; i < pushTransactionThreads; i++ {
		go signalSlave(manachan, tasks, anschan)
	}

	for i := 0; i < nserver -1; i++ {
		ansset := <-anschan
		answer := ansset[0]
		if answer.err == nil {
			return true
		}
	}
	return false
}

func pushBlocks(blockJson string) {
	tasks := []RPCQuery{RPCQuery{apiType:6, arg: &pb.JsonBlockString{blockJson}}}
	manachan := make(chan *Server, nserver+1)
	anschan := make(chan []*RPCAnswer, nserver+1)

	for i := 0; i < nserver; i++ {
		if i != selfid-1 {
			manachan <- &(servermap[i])
		}
	}
	close(manachan)

	for i := 0; i < pushBlockThreads; i++ {
		go signalSlave(manachan, tasks, anschan)
	}
}

func getHeightBlocks(seen map[string]bool) {
	log := logging.MustGetLogger(fmt.Sprintf("gh%03d",rand.Intn(1000)))
	log.Info("New manager for getHeight")

	tasks := []RPCQuery{RPCQuery{apiType:4, arg: &pb.Null{}}}
	manachan := make(chan *Server, nserver+1)
	anschan := make(chan []*RPCAnswer, nserver+1)

	for i := 0; i < nserver; i++ {
		if i != selfid-1 {
			manachan <- &(servermap[i])
		}
	}

	close(manachan)

	for i := 0; i < getHeightThreads; i++ {
		go signalSlave(manachan, tasks, anschan)
	}

	for i := 0; i < nserver -1; i++ {
		ansset := <-anschan

		log.Debug("One Client Response")
		for _, answer := range ansset {
			if answer == nil {
				log.Debug("Connection error")
				continue
			}
			if answer.err != nil {
				log.Debug(answer.err)
				continue
			}
			response, ok := answer.ans.(*pb.GetHeightResponse)
			if !ok {
				log.Debug("Answer format error")
				continue
			}

			log.Debugf("Height: %d, Hash: %s", response.Height, response.LeafHash)

			if _, exist := seen[response.LeafHash]; exist {
				log.Debugf("Exist this block")
				continue
			}

			height := blockchain.longest.block.BlockID

			log.Debugf("My Longest Height: %d", height)


			if height >= response.Height {
				continue
			} else {
				seen[response.LeafHash] = true
				go queryBlocks([]string{response.LeafHash})
			}
		}
	}
	log.Debug("Manager end")
}