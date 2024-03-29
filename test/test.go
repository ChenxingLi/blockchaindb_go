package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"time"
	"io/ioutil"
	"log"
	"math/rand"
	"strconv"
	"os"
	"os/exec"

	hashapi "../hash"
	pb "../protobuf/go"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"github.com/golang/protobuf/jsonpb"
)

var debug bool = true
var passed_test = 0
var total_test = 0
var jsonpbm = jsonpb.Marshaler{}

var ips = func() []string {
	conf, err := ioutil.ReadFile("../server/config.json")
	if err != nil {
		panic(err)
	}
	var dat map[string]interface{}
	err = json.Unmarshal(conf, &dat)
	if err != nil {
		panic(err)
	}
	nservers := int(dat["nservers"].(float64))
	fmt.Println(nservers)
	ips := make([]string, nservers)
	for i := 0; i < nservers; i++ {
		d := dat[strconv.Itoa(i+1)].(map[string]interface{})
		ips[i] = fmt.Sprintf("%s:%s", d["ip"], d["port"])
		fmt.Println(ips[i])
	}
	return ips
}()

var (
	OpCode = flag.String("T", "", `DB transactions to perform:
	1 (or GET):      Show the balance of a given UserID. 
		Require option -user.
	5 (or TRANSFER): Transfer some money from one account to another.
		Require option -from, -to, -value.
	// 6 (or VERIFY): ...
	`)
	UserID = flag.String("user", "00000000", "User account ID for the operation.")
	FromID = flag.String("from", "00000000", "From account (for Transfer)")
	ToID   = flag.String("to", "12345678", "To account (for Transfer)")
	Value  = flag.Int("value", 1, "Amount of transaction")
	Fee    = flag.Int("fee", 1, "Mining Fee of transaction")
)

func UUID128bit() string {
	// Returns a 128bit hex string, RFC4122-compliant UUIDv4
	u := make([]byte, 16)
	_, _ = rand.Read(u)
	// this make sure that the 13th character is "4"
	u[6] = (u[6] | 0x40) & 0x4F
	// this make sure that the 17th is "8", "9", "a", or "b"
	u[8] = (u[8] | 0x80) & 0xBF
	return fmt.Sprintf("%x", u)
}

func NewClient(address string) (pb.BlockChainMinerClient, *grpc.ClientConn) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())

	if err != nil {
		log.Fatalf("Cannot connect to server: %v", err)
	}
	//new client
	client := pb.NewBlockChainMinerClient(conn)
	return client, conn
}

func Assert(a, b interface{}) {
	total_test++
	if a != b {
		log.Printf("[ERROR] Assertion failed, %v != %v", a, b)
	} else {
		passed_test++
	}
}

func Get(c pb.BlockChainMinerClient, UserID string) int {
	if r, err := c.Get(context.Background(), &pb.GetRequest{UserID: fill(UserID)}); err != nil {
		log.Printf("GET Error: %v", err)
		return -1
	} else {
		if debug {
			log.Printf("GET Return: %d", r.Value)
		}
		return int(r.Value)
	}
}

func Transfer(c pb.BlockChainMinerClient, FromID string, ToID string, Value int32, Fee int32) (bool, string) {
	UUID := UUID128bit()
	start := time.Now()
	if r, err := c.Transfer(context.Background(), &pb.Transaction{
		Type:   pb.Transaction_TRANSFER,
		UUID:   UUID,
		FromID: fill(FromID), ToID: fill(ToID), Value: int32(Value), MiningFee: int32(Fee)}); err != nil {
		log.Printf("TRANSFER Error: %v", err)
		return false, UUID
	} else {
		if debug {
			log.Printf("TRANSFER Return: %v", r.Success)
		}
		elapsed := time.Since(start)
		log.Printf("Transfer took %s", elapsed)
		return r.Success, UUID
	}
}

func fill(s string) string {
	for len(s) < 8{
		s += "."
	}
	return s
}

func Verify(c pb.BlockChainMinerClient, FromID string, ToID string, Value int32, Fee int32, UUID string) *pb.VerifyResponse {
	if r, err := c.Verify(context.Background(), &pb.Transaction{
		Type:      pb.Transaction_TRANSFER,
		FromID:    fill(FromID),
		ToID:      fill(ToID),
		Value:     Value,
		MiningFee: Fee,
		UUID:      UUID}); err != nil {
		log.Printf("VERIFY Error: %v", err)
		return r
	} else {
		if debug {
			log.Printf("VERIFY Return: %v", *r)
		}
		return r
	}
}

func GetHeight(c pb.BlockChainMinerClient) *pb.GetHeightResponse {
	if r, err := c.GetHeight(context.Background(), &pb.Null{}); err != nil {
		log.Printf("GET_HEIGHT Error: %v", err)
		return r
	} else {
		if debug {
			log.Printf("GET_HEIGHT Return: %v", r)
		}
		return r
	}
}

func GetBlock(c pb.BlockChainMinerClient, BlockHash string) string {
	if r, err := c.GetBlock(context.Background(), &pb.GetBlockRequest{BlockHash: BlockHash}); err != nil {
		log.Printf("GET_BLOCK Error: %v", err)
		return ""
	} else {
		if debug {
			log.Printf("GET_BLOCK Return: %v", r.Json)
		}
		return r.Json
	}
}

func WaitForPending(client pb.BlockChainMinerClient, FromID string, ToID string, Value int32, Fee int32, UUID string) bool {
	for t := 0; t < 20; t++ {
		result := Verify(client, FromID, ToID, Value, Fee, UUID)
		if result.Result.String() == "SUCCEEDED" {
			return true
		} else if result.Result.String() == "PENDING" {
			if result.BlockHash == "" {
				time.Sleep(time.Second)
			}
			return true
		} else {
			time.Sleep(time.Second)
		}
	}
	return false
}

func FinishTest() {
	client, conn := NewClient(ips[0])
	defer conn.Close()

	var success bool
	UUIDs := make([]string, 300)

	time.Sleep(time.Second * 3)
	for i := 0; i < 300; i++ {
		success, UUIDs[i] = Transfer(client, "xxx", "yyy", 2, 1)
		Assert(success, true)
	}
	for i := 0; i < 300; i++ {
		Assert(WaitForPending(client, "xxx", "yyy", 2, 1, UUIDs[i]), true)
	}
	time.Sleep(time.Second * 3)
}

func BasicTest() {
	fmt.Println("Starting basic test...")
	nservers := len(ips)
	clients := make([]pb.BlockChainMinerClient, nservers)
	conns := make([]*grpc.ClientConn, nservers)
	for i := 0; i < nservers; i++ {
		clients[i], conns[i] = NewClient(ips[i])
		defer conns[i].Close()
	}

	note := make(chan int, 1)

	go func(c chan int) {
		loop := true
		for loop {
			select {
			case <-c:
				loop = false
			default:
			}
			//client := clients[rand.Int() % nservers]
			client := clients[0]
			_ = Verify(client, "a", "b", 2, 1, UUID128bit())
			_ = GetHeight(client)
		}
	}(note)

	n := 50
	m := 30
	c := make(chan bool, m)
	UUIDs := make([]string, n*m*2)

	// use 30 goroutines, each transferring 2 from each of "a000"-"a049" to "b" with mining fee 1
	for j := 0; j < m; j++ {
		go func(c chan bool, j int) {
			for i := 0; i < n*2; i += 2 {
				//client := clients[rand.Int() % nservers]
				client := clients[0]
				name := fmt.Sprintf("a%03d", i/2)
				_, UUIDs[j*2*n+i] = Transfer(client, name, "b", 3, 2)
				_, UUIDs[j*2*n+i+1] = Transfer(client, name, "b", 1001, 1)
			}
			c <- true
		}(c, j)
	}
	for j := 0; j < m; j++ {
		Assert(<-c, true)
	}
	note <- 1
	FinishTest()

	for i := 0; i < nservers; i++ {
		Assert(Get(clients[i], "b"), 1000+n*m)
	}
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("a%03d", i)
		Assert(Get(clients[rand.Int()%nservers], name), 1000-3*m)
	}
	for i := 0; i < n*m*2; i += 2 {
		name := fmt.Sprintf("a%03d", (i/2)%n)
		result := Verify(clients[rand.Int()%nservers], name, "b", 3, 2, UUIDs[i]).Result.String()
		Assert(result, "SUCCEEDED")
		result = Verify(clients[rand.Int()%nservers], name, "b", 1001, 1, UUIDs[i+1]).Result.String()
		Assert(result, "FAILED")
		result = Verify(clients[rand.Int()%nservers], name, "bb", 2, 2, UUIDs[i]).Result.String()
		Assert(result, "FAILED")
		result = Verify(clients[rand.Int()%nservers], name, "b", 3, 1, UUIDs[i]).Result.String()
		Assert(result, "FAILED")
	}

	sum := 0
	for i := 0; i < nservers; i++ {
		name := fmt.Sprintf("Server%02d", i+1)
		sum = sum + Get(clients[rand.Int()%nservers], name) - 1000
	}
	fmt.Println(sum)
	//check the mining fee
	Assert(sum == 2*n*m+300, true)

	response := GetHeight(clients[0])
	height, leafHash := int(response.Height), response.LeafHash
	fmt.Println(height)

	Assert(height >= m +5, true) //check the length of longest chain

	//var prevHash string
	numTransactions := 0
	curHash := leafHash

	for i := 0; i < height; i++ {
		Assert(hashapi.CheckHash(curHash), true) //check the validity of hash value
		blockString := GetBlock(clients[rand.Int()%nservers], curHash)
		Assert(hashapi.GetHashString(blockString), curHash) //check the validity of hash value
		var block pb.Block
		json.Unmarshal([]byte(blockString), &block)
		curHash = block.PrevHash
		numTransactions = numTransactions + len(block.Transactions)
		Assert(int(block.BlockID), height-i) //chekc BlockID
	}
	Assert(curHash, "0000000000000000000000000000000000000000000000000000000000000000") //check it traces back to root
	Assert(numTransactions, n*m+300)                                                    //check the total number of transactions in the chain
}

func StartServers() {
	os.Chdir("../server")
	exec.Command("sh", "-c", "go build").Run()
	nservers := len(ips)
	for i := 0; i < nservers; i++ {
		s := fmt.Sprintf("./server --id=%d", i+1)
		fmt.Println(s)
		cmd := exec.Command("sh", "-c", s)
		//cmd.Stdout = os.Stdout
		//cmd.Stderr = os.Stderr
		cmd.Start()
	}
	os.Chdir("../test")
	time.Sleep(time.Second * 2)
}

func ShutServers() {
	exec.Command("sh", "-c", "pkill server").Run()
}

func ClearData() {
	exec.Command("sh", "-c", "./clear.sh").Run()
}

func main() {
	flag.Parse()
	rand.Seed(int64(time.Now().Nanosecond()))
	fmt.Println(UUID128bit())

	//// Set up a connection to the server.
	// ShutServers()
	// ClearData()
	// StartServers()

	BasicTest()

	ShutServers()
	ClearData()

	fmt.Println("================================================================")
	fmt.Println(fmt.Sprintf("Pass %d/%d tests", passed_test, total_test))
}
