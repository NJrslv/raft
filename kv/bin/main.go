package main

import (
	"flag"
	"log"
	"net/http"
	"raft"
	"raft/kv"
)

func parseID() int32 {
	var id int
	flag.IntVar(&id, "id", -1, "ID of the item")
	flag.Parse()

	if flag.Lookup("id") == nil {
		log.Fatal("MAIN(FATAL): missing required flag --id")
	}

	return int32(id)
}

/*
./kvapi --id 1
*/

func main() {
	database := make(map[string]string)

	id := parseID()
	server := raft.NewServer(id, &database)
	server.StartServer()

	kvApi := kv.NewKVapi(server, &database)

	http.HandleFunc("/set", kvApi.SetHandler)
	http.HandleFunc("/get", kvApi.GetHandler)

	kvAddress, ok := raft.FindServerAddressByID(id)
	if !ok {
		log.Fatal("MAIN(FATAL): incorrect server id")
	}

	err := http.ListenAndServe(":"+raft.GetPort(kvAddress), nil)
	if err != nil {
		log.Fatal("MAIN(FATAL): " + err.Error())
	}
}
