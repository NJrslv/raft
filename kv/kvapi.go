package kv

import (
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
	key := r.URL.Query().Get("key")
	if key == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Fatal("KVAPI(FATAL): missing key param in GET request")
	}

	// if we have multiple clients
	kv.lock.Lock()
	defer kv.lock.Unlock()

	if kv.raftServer.IsLeader() {
		command := raft.CommandKV{Op: raft.Set, Key: key, Value: ""}
		value, ok := kv.raftServer.Submit(command)
		if ok {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(value))
			log.Printf("KVAPI: got value for key '%s'", key)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
			log.Printf("KVAPI(WARN): error getting value for key '%s'", key)
		}
	} else {
		currentLeader := kv.raftServer.GetLeaderAddress()
		http.Redirect(w, r, "http://"+currentLeader+"/get?key="+key, http.StatusTemporaryRedirect)
		log.Printf("KVAPI: redirected GET request for key '%s' to leader at %s\n", key, currentLeader)
	}
}

func (kv *KVapi) SetHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	val := r.URL.Query().Get("val")
	if key == "" || val == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Fatal("KVAPI(FATAL): missing key or val param in SET request")
	}

	// if we have multiple clients
	// это может убрать я про lock у kv
	kv.lock.Lock()
	defer kv.lock.Unlock()

	if kv.raftServer.IsLeader() {
		command := raft.CommandKV{Op: raft.Set, Key: key, Value: val}
		_, ok := kv.raftServer.Submit(command)
		if ok {
			w.WriteHeader(http.StatusOK)
			log.Printf("KVAPI: set value for key '%s' value '%s", key, val)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
			log.Printf("KVAPI(WARN): error setting value '%s' for key '%s'", val, key)
		}
	} else {
		currentLeader := kv.raftServer.GetLeaderAddress()
		http.Redirect(w, r, "http://"+currentLeader+"/get?key="+key, http.StatusTemporaryRedirect)
		log.Printf("KVAPI: redirected SET request for key '%s' to leader at %s\n", key, currentLeader)
	}
}
