package main

import (
	pb "../protobuf/go"
	"golang.org/x/net/context"
	"fmt"
	"github.com/op/go-logging"
	"math/rand"
)

type rpcserver struct{}

// Return UserID's Balance on the Chain, after considering the latest valid block. Pending transactions have no effect on Get()
func (c *rpcserver) Get(ctx context.Context, in *pb.GetRequest) (*pb.GetResponse, error) {
	log := logging.MustGetLogger(fmt.Sprintf("%05d", rand.Intn(100000)))
	log.Infof("[RPCRequest][Get]%v", in)
	blockchain.lock1.RLock()
	defer blockchain.lock1.RUnlock()
	answer := blockchain.longest.accounts.check_nosync(in.UserID)
	log.Info("[RPCReturn][Get]", answer)
	return &pb.GetResponse{Value: answer}, nil
}

// Receive and Broadcast Transaction: balance[FromID]-=Value, balance[ToID]+=(Value-MiningFee), balance[MinerID]+=MiningFee
// Return Success=false if FromID is same as ToID or latest balance of FromID is insufficient
func (c *rpcserver) Transfer(ctx context.Context, in *pb.Transaction) (*pb.BooleanResponse, error) {
	log := logging.MustGetLogger(fmt.Sprintf("%05d", rand.Intn(100000)))
	log.Infof("[RPCRequest][Tra]%v", in)
	if in.FromID == in.ToID || in.MiningFee <= 0 || in.MiningFee >= in.Value {
		return &pb.BooleanResponse{Success: false}, nil
	}

	blockchain.lock1.RLock()
	if !blockchain.longest.accounts.afford_nosync(in.FromID, in.Value) {
		blockchain.lock1.RUnlock()
		log.Info("[RPCReturn][Tra]", false)
		return &pb.BooleanResponse{Success: false}, nil
	}
	blockchain.lock1.RUnlock()

	if !pendingTxs.addTx(in, log) {
		log.Info("[RPCReturn][Tra]", false)
		return &pb.BooleanResponse{Success: false}, nil
	}

	success := pushTransactions(in)
	log.Info("[RPCReturn][Tra]", success)

	return &pb.BooleanResponse{Success: success}, nil
}

// Check if a transaction has been written into a block, or is still waiting, or is invalid on the longest branch.
func (c *rpcserver) Verify(ctx context.Context, in *pb.Transaction) (*pb.VerifyResponse, error) {
	log := logging.MustGetLogger(fmt.Sprintf("%05d", rand.Intn(100000)))
	log.Infof("[RPCRequest][Ver]%v", in)

	blockchain.lock1.RLock()
	defer blockchain.lock1.RUnlock()

	if contain, hash := blockchain.checkUuid_nosync(in.UUID, blockchain.longest, log); contain == 0 {
		if blockchain.longest.accounts.afford_nosync(in.FromID, in.Value) {
			pendingTxs.dataLock.RLock()
			defer pendingTxs.dataLock.RUnlock()
			if _, exist := pendingTxs.uuidmap[in.UUID]; exist {
				log.Info("[RPCReturn][Ver]Pending")
				return &pb.VerifyResponse{Result: pb.VerifyResponse_PENDING}, nil
			} else {
				log.Info("[RPCReturn][Ver]Fail")
				return &pb.VerifyResponse{Result: pb.VerifyResponse_FAILED}, nil
			}
		} else {
			log.Info("[RPCReturn][Ver]Fail")
			return &pb.VerifyResponse{Result: pb.VerifyResponse_FAILED}, nil
		}
	} else if contain+6 <= blockchain.longest.block.BlockID {
		log.Info("[RPCReturn][Ver]Success")
		return &pb.VerifyResponse{Result: pb.VerifyResponse_SUCCEEDED, BlockHash: hash}, nil
	} else {
		log.Info("[RPCReturn][Ver]Pending")
		return &pb.VerifyResponse{Result: pb.VerifyResponse_PENDING, BlockHash: hash}, nil
	}

}

// Get the current blockchain length; use the longest branch if multiple branch exist.
func (c *rpcserver) GetHeight(ctx context.Context, in *pb.Null) (*pb.GetHeightResponse, error) {
	log := logging.MustGetLogger(fmt.Sprintf("%05d", rand.Intn(100000)))
	log.Infof("[RPCRequest][Hei]%v", in)
	blockchain.lock1.RLock()
	defer blockchain.lock1.RUnlock()
	log.Infof("[RPCReturn][Hei]%d, %s", blockchain.longest.block.BlockID, blockchain.longest.hash)
	return &pb.GetHeightResponse{Height: blockchain.longest.block.BlockID, LeafHash: blockchain.longest.hash}, nil
}

// Get the Json representation of the block with BlockHash hash value
func (c *rpcserver) GetBlock(ctx context.Context, in *pb.GetBlockRequest) (*pb.JsonBlockString, error) {
	log := logging.MustGetLogger(fmt.Sprintf("%05d", rand.Intn(100000)))
	log.Infof("[RPCRequest][GBk]%v", in)
	hash := in.BlockHash
	if len(hash) != 64 || hash == zeroHash {
		log.Infof("[RPCReturn][GBk]")
		return &pb.JsonBlockString{}, nil
	}

	blockchain.lock1.RLock()
	defer blockchain.lock1.RUnlock()

	value, success := blockchain.blocks[hash]
	if success {
		log.Infof("[RPCReturn][GBk]%v", pb.JsonBlockString{Json: value.json})
		return &pb.JsonBlockString{Json: value.json}, nil
	} else {
		log.Infof("[RPCReturn][GBk]%v", pb.JsonBlockString{})
		return &pb.JsonBlockString{}, nil
	}
}

// Send a block to another server
func (c *rpcserver) PushBlock(ctx context.Context, in *pb.JsonBlockString) (*pb.Null, error) {
	log := logging.MustGetLogger(fmt.Sprintf("%05d", rand.Intn(100000)))
	log.Infof("[RPCRequest][PhB]%v", in)
	if block, err := blockchain.parse(in); err == nil {
		blockchain.add(block, false, log)
	} else {
		log.Warning("[          ]", err)
	}
	log.Infof("[RPCReturn][PhB]")

	return &pb.Null{}, nil
}

// Send a transaction to another server
func (c *rpcserver) PushTransaction(ctx context.Context, in *pb.Transaction) (*pb.Null, error) {
	log := logging.MustGetLogger(fmt.Sprintf("%05d", rand.Intn(100000)))
	log.Infof("[RPCRequest][PhT]%v", in)
	if in.FromID == in.ToID || in.MiningFee <= 0 || in.MiningFee >= in.Value {
		log.Infof("[RPCReturn][PhT]")
		return &pb.Null{}, nil
	}

	blockchain.lock1.RLock()
	if !blockchain.longest.accounts.afford_nosync(in.FromID, in.Value) {
		blockchain.lock1.RUnlock()
		log.Infof("[RPCReturn][PhT]")
		return &pb.Null{}, nil
	}
	blockchain.lock1.RUnlock()

	pendingTxs.addTx(in, log)
	log.Infof("[RPCReturn][PhT]")
	return &pb.Null{}, nil
}
