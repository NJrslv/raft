package kv

import (
	"fmt"
	"log"
	"net/http"
	"raft"
	"sync"
)

type KVapi struct {
	raftServer *raft.Server
	lock       sync.Mutex
	db         *map[string]string
}

func NewKVapi(server *raft.Server, db *map[string]string) *KVapi {
	return &KVapi{
		raftServer: server,
		db:         db,
	}
}

func (kv *KVapi) GetHandler(w http.ResponseWriter, r *http.Request) {
	kv.lock.Lock()
	defer kv.lock.Unlock()

	key := r.URL.Query().Get("key")
	if key == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Fatal("KVAPI(FATAL): missing key param in GET request")
	}

	command := raft.CommandKV{Op: raft.Get, Key: key, Value: ""}
	response := kv.raftServer.Submit(command)
	if response.Ok {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("get value: " + response.Msg))
		log.Printf("KVAPI: got value for key '%s': %s", key, response.Msg)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("not a leader or error"))
		log.Printf("KVAPI(WARN): error getting value for key '%s'", key)
	}

	// Redirect:
	//currentLeader := kv.raftServer.GetLeaderAddress()
	//http.Redirect(w, r, "http://"+currentLeader+"/get?key="+key, http.StatusTemporaryRedirect)
	//log.Printf("KVAPI: redirected GET request for key '%s' to leader at %s\n", key, currentLeader)
}

func (kv *KVapi) SetHandler(w http.ResponseWriter, r *http.Request) {
	kv.lock.Lock()
	defer kv.lock.Unlock()

	key := r.URL.Query().Get("key")
	val := r.URL.Query().Get("val")
	if key == "" || val == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Fatal("KVAPI(FATAL): missing key or val param in SET request")
	}

	command := raft.CommandKV{Op: raft.Set, Key: key, Value: val}
	response := kv.raftServer.Submit(command)
	if response.Ok {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("set value"))
		log.Printf("KVAPI: set value for key '%s' value '%s", key, val)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("not a leader or error"))
		log.Printf("KVAPI(WARN): error setting value '%s' for key '%s'", val, key)
	}

	// Redirect
	//currentLeader := kv.raftServer.GetLeaderAddress()
	//http.Redirect(w, r, "http://"+currentLeader+"/get?key="+key, http.StatusTemporaryRedirect)
	//log.Printf("KVAPI: redirected SET request for key '%s' to leader at %s\n", key, currentLeader)
}

func (kv *KVapi) PrintRunInstruction() {
	fmt.Print("Run all processes listed in the configuration file:\n")
	for _, serverConfig := range raft.Cluster {
		fmt.Printf("$ ./kv --id %d\n", serverConfig.Id)
	}
}
