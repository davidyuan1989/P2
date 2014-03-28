package storageserver

import (
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"hash/fnv"
	"net/rpc"
	"strings"
	"sync"
	"time"
)

type storageServer struct {
	// TODO: implement this!
	tribbleHash       map[string][]byte
	listHash          map[string]*list.List
	portmun           int
	nodeID            uint32
	isMaster          bool
	nodes             map[storagerpc.Node]bool
	modification      map[string]bool
	modificationList  map[string]bool
	leases            map[string]*list.List
	leasesList        map[string]*list.List
	connections       map[string]*rpc.Client
	numNodes          int
	connectionLock    *sync.Mutex
	leaseLocker       *sync.Mutex
	leaseListLocker   *sync.Mutex
	modifyingLock     *sync.Mutex
	modifyingListLock *sync.Mutex
	registeredLocker  *sync.Mutex
	storageLocker     *sync.Mutex
	listLocker        *sync.Mutex
}

type leaseInfo struct {
	address    string
	expiryTime time.Time
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// nodeID is a random, unsigned 32-bit ID identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {
	serverNode := &storageServer{
		tribbleHash:      make(map[string][]byte),
		listHash:         make(map[string]*list.List),
		portmun:          port,
		nodeID:           nodeID,
		numNodes:         numNodes,
		registeredLocker: new(sync.Mutex),
		storageLocker:    new(sync.Mutex),
		listLocker:       new(sync.Mutex),
	}
	if masterServerHostPort != "" {
		serverNode.numNodes = numNodes
		serverNode.isMaster = true
		serverNode.nodes = make(map[storagerpc.Node]bool)
		master := storagerpc.Node{masterServerHostPort, nodeID}
		serverNode.nodes[master] = true
	} else {
		masterNode, _ := rpc.DialHTTP("tcp", masterServerHostPort)
		var regArgs storagerpc.RegisterArgs
		var regReply storagerpc.RegisterReply
		regArgs.ServerInfo.HostPort = fmt.Sprintf("localhost:%d", port)
		regArgs.ServerInfo.NodeID = nodeID
		serverNode.isMaster = false
		for {
			masterNode.Call("storageServer.RegisterServer", &regArgs, &regReply)
			if regReply.Status != storagerpc.OK {
				time.Sleep(1000 * time.Millisecond)
			} else {
				break
			}
		}
	}
	return serverNode, nil
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	if !ss.isMaster {
		return errors.New("Not a master!")
	}
	ss.registeredLocker.Lock()
	defer ss.registeredLocker.Unlock()
	ss.nodes[args.ServerInfo] = true
	if ss.numNodes == len(ss.nodes) {
		reply.Servers = make([]storagerpc.Node, ss.numNodes)
		reply.Status = storagerpc.OK
		i := 0
		for node, _ := range ss.nodes {
			reply.Servers[i] = node
			i++
		}
	} else {
		reply.Status = storagerpc.NotReady
	}
	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	ss.registeredLocker.Lock()
	defer ss.registeredLocker.Unlock()
	if ss.numNodes == len(ss.nodes) {
		reply.Servers = make([]storagerpc.Node, ss.numNodes)
		reply.Status = storagerpc.OK
		i := 0
		for node, _ := range ss.nodes {
			reply.Servers[i] = node
			i++
		}
	} else {
		reply.Status = storagerpc.NotReady
	}
	return nil

}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	if !ss.CheckRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	if args.WantLease {
		ss.modifyingLock.Lock()
		if ss.modification[args.Key] == true {
			reply.Lease.Granted = false
		} else {
			reply.Lease.Granted = true
			ss.leaseLocker.Lock()
			lst := ss.leases[args.Key]
			if lst == nil {
				lst = list.New()
			}
			lst.PushBack(&leaseInfo{args.HostPort, time.Now().Add((storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds) * time.Second)})
			ss.leaseLocker.Unlock()
		}
		ss.modifyingLock.Unlock()
	}
	ss.storageLocker.Lock()
	defer ss.storageLocker.Unlock()
	if ss.tribbleHash[args.Key] == nil {
		reply.Status = storagerpc.KeyNotFound
	} else {
		var value string
		json.Unmarshal(ss.tribbleHash[args.Key], &value)
		reply.Value = value
		reply.Status = storagerpc.OK
	}
	return nil
}
func (ss *storageServer) CheckRange(Key string) bool {
	if len(ss.nodes) == 1 {
		return true
	}
	hostName := strings.Split(Key, ":")
	hash := StoreHash(hostName[0])
	if ss.getCurNode(hash) == ss.nodeID {
		return true
	} else {
		return false
	}
}

func (ss *storageServer) getCurNode(hash uint32) uint32 {
	smallNode := ss.nodeID
	for node, _ := range ss.nodes {
		if smallNode > node.NodeID {
			smallNode = node.NodeID
		}
	}
	var curNode *uint32
	for node, _ := range ss.nodes {
		if node.NodeID >= hash && (curNode == nil || node.NodeID < *curNode) {
			*curNode = node.NodeID
		}
	}
	if curNode == nil {
		curNode = &smallNode
	}
	return *curNode
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	if !ss.CheckRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	if args.WantLease {
		ss.modifyingListLock.Lock()
		if ss.modificationList[args.Key] == true {
			reply.Lease.Granted = false
		} else {
			reply.Lease.Granted = true
			ss.leaseListLocker.Lock()
			lst := ss.leasesList[args.Key]
			if lst == nil {
				lst = list.New()
			}
			lst.PushBack(&leaseInfo{args.HostPort, time.Now().Add((storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds) * time.Second)})
			ss.leaseListLocker.Unlock()
		}
		ss.modifyingListLock.Unlock()
	}

	ss.listLocker.Lock()
	defer ss.listLocker.Unlock()
	if ss.listHash[args.Key] == nil {
		reply.Status = storagerpc.KeyNotFound
	} else {
		valueList := ss.listHash[args.Key]
		reply.Value = make([]string, valueList.Len())
		i := 0
		for e := valueList.Front(); e != nil; e = e.Next() {
			reply.Value[i] = e.Value.(string)
			i++
		}
		reply.Status = storagerpc.OK
	}
	return nil

}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.CheckRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.modifyingLock.Lock()
	ss.modification[args.Key] = true
	ss.modifyingLock.Unlock()

	ss.revokeLease(args.Key, false)
	ss.storageLocker.Lock()
	ss.tribbleHash[args.Key], _ = json.Marshal(args.Value)
	ss.storageLocker.Unlock()

	ss.modifyingLock.Lock()
	delete(ss.modification, args.Key)
	ss.modifyingLock.Unlock()
	reply.Status = storagerpc.OK
	return nil
}

func (ss *storageServer) revokeLease(key string, isList bool) {
	var list *list.List
	if isList {
		ss.leaseListLocker.Lock()
		list = ss.leasesList[key]
		ss.leaseListLocker.Unlock()
	} else {
		ss.leaseLocker.Lock()
		list = ss.leases[key]
		ss.leaseLocker.Unlock()
	}
	if list == nil {
		return
	}
	for e := list.Front(); e != nil; e = e.Next() {
		info := e.Value.(*leaseInfo)
		if !time.Now().After(info.expiryTime) {
			ss.connectLibStore(key, info)
		}
	}
	if isList {
		ss.leaseListLocker.Lock()
		delete(ss.leasesList, key)
		ss.leaseListLocker.Unlock()
	} else {
		ss.leaseLocker.Lock()
		delete(ss.leases, key)
		ss.leaseLocker.Unlock()
	}
}

func (ss *storageServer) connectLibStore(key string, info *leaseInfo) {
	cli, e := rpc.DialHTTP("tcp", info.address)
	for e != nil && !time.Now().After(info.expiryTime) {
		cli, e = rpc.DialHTTP("tcp", info.address)
	}
	args := &storagerpc.RevokeLeaseArgs{key}
	var reply *storagerpc.RevokeLeaseReply
	e = cli.Call("RemoteLeaseCallbacks.RevokeLease", args, reply)
	for e != nil && !time.Now().After(info.expiryTime) {
		e = cli.Call("RemoteLeaseCallbacks.RevokeLease", args, reply)
	}
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.CheckRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.modifyingListLock.Lock()
	ss.modificationList[args.Key] = true
	ss.modifyingListLock.Unlock()
	ss.revokeLease(args.Key, true)

	ss.storageLocker.Lock()
	curList := ss.listHash[args.Key]
	if curList == nil {
		ss.listHash[args.Key] = list.New()
		curList = ss.listHash[args.Key]
	} else {
		for e := curList.Front(); e != nil; e = e.Next() {
			if e.Value.(string) == args.Value {
				reply.Status = storagerpc.ItemExists
				ss.storageLocker.Unlock()
				return nil
			}
		}
	}
	curList.PushBack(args.Value)
	ss.storageLocker.Unlock()
	reply.Status = storagerpc.OK

	ss.modifyingListLock.Lock()
	delete(ss.modificationList, args.Key)
	ss.modifyingListLock.Unlock()

	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.CheckRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.modifyingListLock.Lock()
	ss.modificationList[args.Key] = true
	ss.modifyingListLock.Unlock()
	ss.revokeLease(args.Key, true)

	ss.storageLocker.Lock()
	curList := ss.listHash[args.Key]
	if curList != nil {
		for e := curList.Front(); e != nil; e = e.Next() {
			if e.Value.(string) == args.Key {
				curList.Remove(e)
				ss.storageLocker.Unlock()
				reply.Status = storagerpc.OK
				return nil
			}
		}
	}
	ss.storageLocker.Unlock()
	reply.Status = storagerpc.ItemNotFound

	ss.modifyingListLock.Lock()
	delete(ss.modificationList, args.Key)
	ss.modifyingListLock.Unlock()

	return nil
}

func StoreHash(key string) uint32 {
	hasher := fnv.New32()
	hasher.Write([]byte(key))
	return hasher.Sum32()
}
