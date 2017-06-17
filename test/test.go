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

	pb "../protobuf/go"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var debug bool = false
var passed_test = 0
var total_test = 0

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
	if r, err := c.Get(context.Background(), &pb.GetRequest{UserID: UserID}); err != nil {
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
	if r, err := c.Transfer(context.Background(), &pb.Transaction{
		Type:   pb.Transaction_TRANSFER,
		UUID:   UUID,
		FromID: FromID, ToID: ToID, Value: int32(Value), MiningFee: int32(Fee)}); err != nil {
		log.Printf("TRANSFER Error: %v", err)
		return false, UUID
	} else {
		if debug {
			log.Printf("TRANSFER Return: %v", r.Success)
		}
		return r.Success, UUID
	}
}

func Verify(c pb.BlockChainMinerClient, FromID string, ToID string, Value int32, Fee int32, UUID string) *pb.VerifyResponse {
	if r, err := c.Verify(context.Background(), &pb.Transaction{
		FromID: FromID,
		ToID: ToID,
		Value: Value,
		MiningFee: Fee,
		UUID: UUID}); err != nil {
		log.Printf("VERIFY Error: %v", err)
		return r
	} else {
		if debug {
			log.Printf("VERIFY Return: %v", r)
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
	for t := 0; t < 20; t++{
		result := Verify(client, FromID, ToID, Value, Fee, UUID).Result
		if result.String() == "SUCCEEDED" {
			return true
		} else if result.String() == "PENDING" {
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
		success, UUIDs[i] = Transfer(client,"xxx","yyy",2,1)
		Assert(success, true)
	}
	for i := 0; i < 300; i++ {
		Assert(WaitForPending(client, "xxx", "yyy", 2, 1,UUIDs[i]), true)
	}
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

	n := 50
	m := 30
	c := make(chan bool, m)
	UUIDs := make([]string, n * m)

	for j := 0; j < m; j++ {
		go func(c chan bool, j int) {
			for i := 0; i < n; i++ {
				//client := clients[rand.Int() % nservers]
				client := clients[0]
				name := fmt.Sprintf("a%03d", i)
				_, UUIDs[j * n + i] = Transfer(client, name, "b", 2, 1)
			}
			c <- true
		}(c, j)
	}
	for j := 0; j < m; j++ {
		Assert(<-c, true)
	}
	FinishTest()

	for i := 0; i < nservers; i++ {
		Assert(Get(clients[i], "b"), 1000 + n * m)
	}
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("a%03d", i)
		Assert(Get(clients[rand.Int() % nservers], name), 1000 - 2 * m)	
	}
	for i := 0; i < n * m; i++ {
		name := fmt.Sprintf("a%03d", i % n)
		result := Verify(clients[rand.Int() % nservers], name, "b", 2, 1, UUIDs[i]).Result.String()
		Assert(result, "SUCCEEDED")
	}

	sum := 0
	for i := 0; i < nservers; i++ {
		name := fmt.Sprintf("Server%02d", i+1)
		sum =  sum + Get(clients[rand.Int() % nservers], name) - 1000
	}
	fmt.Println(sum)
	Assert(sum >= n * m, true)
	Assert(sum <= n * m + 300, true)

	response := GetHeight(clients[0])
	height, leafHash := response.Height, response.LeafHash
	fmt.Println(height)
	_ = leafHash
	Assert(int(height) >= m + 5, true)
}

func StartServers() {
	os.Chdir("../server")
	exec.Command("sh","-c","go build").Run()
	nservers := len(ips)
	for i := 0; i < nservers; i++ {
		s := fmt.Sprintf("./server --id=%d", i+1)
		fmt.Println(s)
		cmd := exec.Command("sh","-c",s)
		//cmd.Stdout = os.Stdout
		//cmd.Stderr = os.Stderr
		cmd.Start()
	}
	os.Chdir("../test")
	time.Sleep(time.Second * 2)
}

func ShutServers() {
	exec.Command("sh","-c","pkill server").Run()
}

func ClearData() {
	exec.Command("sh","-c","./clear.sh").Run()	
}

func main() {
	flag.Parse()
	rand.Seed(int64(time.Now().Nanosecond()))
	fmt.Println(UUID128bit())

	// Set up a connection to the server.
	// ShutServers()
	// ClearData()
	// StartServers()

	BasicTest()

	//ShutServers()

	fmt.Println(passed_test, total_test)
}
