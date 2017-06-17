package main

import (
	"io/ioutil"
	"encoding/json"
	"fmt"
	"github.com/op/go-logging"
	"flag"
	"net"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc"
	pb "../protobuf/go"
	"os"
	"math/rand"
	"runtime"
	"github.com/golang/protobuf/jsonpb"
)



var communicationThreads = func() int {
	if runtime.NumCPU() >= 4 {
		return 2
	} else {
		return 1
	}
} ()
var minerThreads = func() int {
	return runtime.NumCPU()
	if runtime.NumCPU() > 4 {
		return runtime.NumCPU() - 2
	} else if runtime.NumCPU() > 1 {
		return runtime.NumCPU() - 1
	} else {
		return 1
	}
}()

var (
	queryBlockThreads      = communicationThreads
	pushTransactionThreads = communicationThreads
	pushBlockThreads       = communicationThreads
	getHeightThreads       = communicationThreads
)

const initBalance = 1000
const pendingFilterThreshold = 100000
const blockLimit = 50
const timeoutSencond = 20
const loglevel = logging.DEBUG

var blockchain *BlockChain
var pendingTxs *PendingTransactions
var miner *Miner

var servermap []Server
var nserver int
var selfid int

var log = logging.MustGetLogger("main")
var pbMarshal = jsonpb.Marshaler{}
var pbUnmarshal = jsonpb.Unmarshaler{}

func initalize() {
	blockchain = newBlockChain()
	pendingTxs = newPendingTransactions()
	miner = newMiner()

	log.Info("Load Blockchain")
	blockchain.prepare()
	log.Info("Load Pending Tx")
	pendingTxs.prepare()

	go blockchain.run()
	go pendingTxs.run()
	go miner.run_producer()
}

func loadConfig() {
	conf, err := ioutil.ReadFile("config.json")

	if err != nil {
		fmt.Print("Can't open config file")
	}

	var dat map[string]interface{}
	_ = json.Unmarshal(conf, &dat)

	nserver = int(dat["nservers"].(float64))

	if lv, err := logging.LogLevel(dat["loglevel"].(string)); err == nil {
		loadLogger(lv)
	} else {
		loadLogger(loglevel)
	}

	log.Infof("nserver=%d", nserver)

	for i := 1; i <= nserver; i++ {
		id := fmt.Sprintf("%d", i)
		server, _ := dat[id].(map[string]interface{})
		servermap = append(servermap, Server{
			ip:      server["ip"].(string),
			port:    server["port"].(string),
			dataDir: server["dataDir"].(string),
			id:      i,
		})
		log.Infof("%s:%s, %s", servermap[i-1].ip, servermap[i-1].port, servermap[i-1].dataDir)
	}
	log.Debugf("len(servermap)=%d", len(servermap))
}

func startServer() {
	// Bind to port
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%s", servermap[selfid-1].ip, servermap[selfid-1].port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Infof("Listening: %s:%s ...", servermap[selfid-1].ip, servermap[selfid-1].port)

	// Create gRPC server
	s := grpc.NewServer()
	pb.RegisterBlockChainMinerServer(s, &rpcserver{})
	// Register reflection service on gRPC server.
	reflection.Register(s)

	// Start server
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func main() {

	randstring := fmt.Sprintf("%x", rand.Int())
	sha256o = (GetHashString(randstring) == GetFastHashString(randstring))
	var pid = flag.Int("id", 1, "Server's ID, 1<=ID<=NServers")
	flag.Parse()

	loadConfig()

	selfid = *pid
	log.Infof("server_id=%d", selfid)
	if selfid > len(servermap) || selfid <= 0 {
		log.Fatal("Invalid self id")
	}
	_, err := os.Stat(servermap[selfid-1].dataDir)
	if err != nil {
		os.MkdirAll(servermap[selfid-1].dataDir, os.ModePerm)
	}

	log.Notice("******************Start*******************")

	initalize()
	startServer()

}
