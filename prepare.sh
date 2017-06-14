go get github.com/op/go-logging
cp -r ./sha256-simd-master $GOPATH/src
cd $GOPATH/src/sha256-simd-master
go install
