package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"raft"
	"raft/kv"
	"syscall"
)

var (
	raftServerId int
	kvAddr       string
)

func parseCMD() {
	flag.IntVar(&raftServerId, "id", -1, "Id of the item")
	flag.StringVar(&kvAddr, "kvaddr", "", "KV address")
	flag.Parse()

	if raftServerId == -1 {
		log.Fatal("missing required flag --id")
	}

	if kvAddr == "" {
		log.Fatal("missing required flag --kvaddr")
	}
}

func startHTTPServer(kvAddress string) {
	go func() {
		err := http.ListenAndServe(":"+raft.GetPort(kvAddress), nil)
		if err != nil {
			log.Fatal(err.Error())
		}
	}()
}

func main() {
	database := make(map[string]string)
	parseCMD()
	server := raft.NewServer(int32(raftServerId), &database)
	kvApi := kv.NewKVapi(server, &database)
	kvApi.PrintRunInstruction()

	startHTTPServer(kvAddr)
	http.HandleFunc("/set", kvApi.SetHandler)
	http.HandleFunc("/get", kvApi.GetHandler)

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)

	go server.Start()
	<-done
	server.Stop()
}
