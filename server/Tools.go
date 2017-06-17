package main

import (
	hashapi "../hash"
	"os"
	"github.com/op/go-logging"
	mysha2 "../sha256-simd-master"
	"fmt"
)

var sha256o bool

type Notification struct {
	channel chan int
}

func (self *Notification) sleep() {
	<-self.channel
}

func (self *Notification) wake() {
	select {
	case self.channel <- 0:
	default:
	}
}

func GetFastHashString(String string) string {
	return fmt.Sprintf("%x", mysha2.Sum256([]byte(String)))
}

func GetHashString(String string) string {
	return hashapi.GetHashString(String)
}

func GetHashBytes(String string) [32]byte {
	return hashapi.GetHashBytes(String)
}

func CheckHash(Hash string) bool {
	return hashapi.CheckHash(Hash)
}

func loadLogger(level logging.Level) {
	formatter := logging.MustStringFormatter(
		"%{color}%{time:15:04:05.0000} [%{level:.4s}] %{shortfile: 19.19s} %{shortfunc: 15.15s} %{module: 5.5s}▶ %{color:reset}%{message} ",
	)
	backend := logging.AddModuleLevel(logging.NewBackendFormatter(logging.NewLogBackend(os.Stdout, "", 0), formatter))
	backend.SetLevel(level, "")

	// Set the backends to be used.
	logging.SetBackend(backend)
}
