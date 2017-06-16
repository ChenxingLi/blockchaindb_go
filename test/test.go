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

	pb "../protobuf/go"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

/*var address = func() string {
	conf, err := ioutil.ReadFile("../server/config.json")
	if err != nil {
		panic(err)
	}
	var dat map[string]interface{}
	err = json.Unmarshal(conf, &dat)
	if err != nil {
		panic(err)
	}
	dat = dat["1"].(map[string]interface{})
	return fmt.Sprintf("%s:%s", dat["ip"], dat["port"])
}()*/

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

var debug bool = true

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
	if a != b {
		log.Fatalf("[ERROR] Assertion failed, %v != %v", a, b)
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

func Verify(c pb.BlockChainMinerClient, UUID string) *pb.VerifyResponse {
	if r, err := c.Verify(context.Background(), &pb.Transaction{}); err != nil {
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

func BasicTest() {
	fmt.Println("Starting basic test...")
	client, conn := NewClient(ips[0])
	defer conn.Close()

	n := 30
	success := false
	UUIDs := make([]string, n)
	for j := 0; j < 30; j++ {
		name := fmt.Sprintf("a%03d", j)
		for i := 0; i < n; i++ {
			success, UUIDs[i] = Transfer(client, name, "b", 2, 1)
			fmt.Println(success)
			fmt.Println(UUIDs[i])
		}
		time.Sleep(time.Duration(1)*time.Second)
	}
	time.Sleep(time.Second * 10)
	Get(client, "a000")
	Get(client, "b")
	Verify(client, UUIDs[0])
	Verify(client, UUIDs[20])
	//Verify(client, UUIDs[49])
	GetHeight(client)
	GetBlock(client, "")
}

func main() {
	flag.Parse()
	rand.Seed(int64(time.Now().Nanosecond()))
	fmt.Println(UUID128bit())

	// Set up a connection to the server.
	BasicTest()
}
