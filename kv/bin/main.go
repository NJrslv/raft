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

func parseID() int32 {
	var id int
	flag.IntVar(&id, "id", -1, "Id of the item")
	flag.Parse()

	if id == -1 {
		log.Fatal("missing required flag --id")
	}
	return int32(id)
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
	id := parseID()

	server := raft.NewServer(id, &database)
	kvApi := kv.NewKVapi(server, &database)
	kvApi.PrintRunInstruction()

	http.HandleFunc("/set", kvApi.SetHandler)
	http.HandleFunc("/get", kvApi.GetHandler)

	kvAddress, ok := raft.FindServerAddressByID(id)
	if !ok {
		log.Fatal("incorrect server id")
	}

	startHTTPServer(kvAddress)

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)

	go server.Start()
	<-done
	server.Stop()
}
