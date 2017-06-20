#!/bin/sh

# This script starts the database server and runs a series of operation against server implementation.
# If the server is implemented correctly, the output (both return values and JSON block) will match the expected outcome.
# Note that this script does not compare the output value, nor does it compare the JSON file with the example JSON.

# Please start this script in a clean environment; i.e. the server is not running, and the data dir is empty.

go run test.go

return

echo "Testrun starting..."

PID=$!
sleep 1

echo "Step 1: Quickly push many transactions"
for I in `seq 0 9`; do
	go run ./test_client.go -T=TRANSFER --from USER000$I --to USER0099 --value=5 --fee=1
done
sleep 10
echo "Check value: expecting value=995"
go run ./test_client.go -T=GET -user=USER0005

echo "Step 2: Slowlu push many transactions, should cause more blocks to be produced"
for I in `seq 0 9`; do
	go run ./test_client.go -T=TRANSFER --from USER000$I --to USER0099 --value=5 --fee=1
	sleep 2
done
echo "You should already see 5~10 blocks."
sleep 10
echo "Check value: expecting value=80"
go run ./test_client.go -T=GET -user=USER0099

echo "Test completed. Please verify BlockChain is legitimate and all earliest transactions are verified."

kill $PID
