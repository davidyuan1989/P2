package tribserver

import (
	"errors"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"strconv"
	"time"
	"net"
	"net/rpc"
	"net/http"
	"encoding/json"
	"sort"
	"strings"
	//"fmt"
	"math/rand"
)

type tribServer struct {
	ls libstore.Libstore
	sID int
	tID int
}

type stringarr []string

func (a stringarr) Len() int {
	return len(a)
}
func (a stringarr) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
	return
}
func (a stringarr) Less(i, j int) bool {
	icolon := strings.Index(a[i], ":")
	islash := strings.Index(a[i], "/")
	jcolon := strings.Index(a[j], ":")
	jslash := strings.Index(a[j], "/")
	iTimeStamp, _ := strconv.ParseInt(a[i][icolon+1:islash], 10, 64)
	jTimeStamp, _ := strconv.ParseInt(a[j][jcolon+1:jslash], 10, 64)
	return iTimeStamp>jTimeStamp
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	ls, err := libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Never)
	if err != nil {
		return nil, errors.New("Could not new libstore")
	}
	ts := &tribServer{}
	ts.ls = ls
	ts.sID = rand.Intn(1000000)
	ts.tID = 1
	listener, err := net.Listen("tcp", myHostPort)
	if err != nil {
		return nil, errors.New("Could not listen to this host port")
	}
	err = rpc.RegisterName("TribServer", tribrpc.Wrap(ts))
	if err != nil {
		return nil, errors.New("Could not register TribServer")
	}
	rpc.HandleHTTP()
	go http.Serve(listener, nil)
	return ts, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	userID := args.UserID
	userKey := userID + ":"
	//tribKey := userID + ":T"
	//subKey := userID + ":S"
	_, err := ts.ls.Get(userKey)
	if err == nil {
		reply.Status = tribrpc.Exists
		return nil
	}
	err = ts.ls.Put(userKey, "")
	if err != nil {
		reply.Status = tribrpc.Exists
		return err
	}
	// err = ts.ls.Put(tribKey, "")
	// if err != nil {
	// 	reply.Status = tribrpc.Exists
	// 	return err
	// }
	// err = ts.ls.Put(subKey, "")
	// if err != nil {
	// 	reply.Status = tribrpc.Exists
	// 	return err
	// }
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	userID := args.UserID
	targetUserID := args.TargetUserID
	userKey := userID + ":"
	targetUserKey := targetUserID + ":"
	_, err := ts.ls.Get(userKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	_, err = ts.ls.Get(targetUserKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	subKey := userID + ":S"
	// subList, err := ts.ls.GetList(subKey)
	// if err == nil {
	// 	targetUserExists := false
	// 	for i:=0; i<len(subList); i++ {
	// 		if strings.EqualFold(subList[i], targetUserID) {
	// 			targetUserExists = true 
	// 			break
	// 		}
	// 	}
	// 	if targetUserExists {
	// 		reply.Status = tribrpc.Exists
	// 		return nil
	// 	}
	// }
	err = ts.ls.AppendToList(subKey, targetUserID)
	if err!=nil {
		if errNum, _ := strconv.Atoi(err.Error()); errNum==int(storagerpc.WrongServer) {
			reply.Status = tribrpc.NoSuchUser
			return nil
		}
		if errNum, _ := strconv.Atoi(err.Error()); errNum==int(storagerpc.ItemExists) {
			reply.Status = tribrpc.Exists
			return nil
		}
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	userID := args.UserID
	targetUserID := args.TargetUserID
	userKey := userID + ":"
	targetUserKey := targetUserID + ":"
	_, err := ts.ls.Get(userKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	_, err = ts.ls.Get(targetUserKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	subKey := userID + ":S"
	// subList, err := ts.ls.GetList(subKey)
	// if err != nil {
	// 	reply.Status = tribrpc.NoSuchUser
	// 	return nil
	// }
	// targetUserExists := false
	// for i:=0; i<len(subList); i++ {
	// 	if strings.EqualFold(subList[i], targetUserID) {
	// 		targetUserExists = true 
	// 		break
	// 	}
	// }
	// if !targetUserExists {
	// 	reply.Status = tribrpc.NoSuchTargetUser
	// 	return nil
	// }
	err = ts.ls.RemoveFromList(subKey, targetUserID)
	if err!=nil {
		if errNum, _ := strconv.Atoi(err.Error()); errNum==int(storagerpc.WrongServer) {
			reply.Status = tribrpc.NoSuchUser
			return nil
		}
		if errNum, _ := strconv.Atoi(err.Error()); errNum==int(storagerpc.ItemNotFound) {
			reply.Status = tribrpc.NoSuchTargetUser
			return nil
		}
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {
	userID := args.UserID
	userKey := userID + ":"
	_, err := ts.ls.Get(userKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	subKey := userID + ":S"
	userIDs, err := ts.ls.GetList(subKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	reply.Status = tribrpc.OK
	reply.UserIDs = userIDs
	return nil
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	userID := args.UserID
	contents := args.Contents
	userKey := userID + ":"
	_, err := ts.ls.Get(userKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	tribble := &tribrpc.Tribble{userID, time.Now(), contents}
	timeStamp := time.Now().UnixNano()
	mTribble, _ := json.Marshal(tribble)
	tribKey := userID + ":T"
	tribbleID := userID + ":" + strconv.FormatInt(timeStamp, 10)+"/"+strconv.Itoa(ts.sID)+"."+strconv.Itoa(ts.tID)
	ts.tID++
	_ = ts.ls.AppendToList(tribKey, tribbleID)
	// if err != nil {
	// 	reply.Status = tribrpc.NoSuchUser
	// 	return nil
	// }
	_ = ts.ls.Put(tribbleID, string(mTribble))
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	userID := args.UserID
	userKey := userID + ":"
	_, err := ts.ls.Get(userKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	tribKey := userID + ":T"
	tribbleIDs, _ := ts.ls.GetList(tribKey)
	if len(tribbleIDs) == 0 {
		reply.Status = tribrpc.OK
		return nil
	}
	// if err!=nil {
	// 	if errNum, _ := strconv.Atoi(err.Error()); errNum==int(storagerpc.WrongServer) {
	// 		reply.Status = tribrpc.NoSuchUser
	// 		return nil
	// 	}
	// 	if errNum, _ := strconv.Atoi(err.Error()); errNum==int(storagerpc.KeyNotFound) {
	// 		reply.Status = tribrpc.NoSuchUser
	// 		return nil
	// 	}
	// }
	// tribbleIDs := make([]int64, len(mTribbleIDs))
	// for i, mTribbleID := range mTribbleIDs {
	// 	//fmt.Println(mTribbleID)
	// 	index := strings.Index(mTribbleID, ":")
	// 	// fmt.Println(index)
	// 	// fmt.Println(mTribbleID[index:])
	// 	tribbleIDs[i], _ = strconv.ParseInt(mTribbleID[index+1:], 10, 64)
	// }
	sort.Sort(stringarr(tribbleIDs))
	length := 100
	if len(tribbleIDs) < 100 {
		length = len(tribbleIDs)
	}
	i := 0
	tribbles := make([]tribrpc.Tribble, length)
	var tribble tribrpc.Tribble
	for i < length {
		mTribble, err := ts.ls.Get(tribbleIDs[i])
		if err == nil {
			json.Unmarshal([]byte(mTribble), &tribble)
			tribbles[i] = tribble
			i++
		}
	}
	reply.Status = tribrpc.OK
	reply.Tribbles = tribbles
	return nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	userID := args.UserID
	userKey := userID + ":"
	_, err := ts.ls.Get(userKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	subKey := userID + ":S"
	subList, _ := ts.ls.GetList(subKey)
	if len(subList) == 0 {
		reply.Status = tribrpc.OK
		return nil
	}
	var allTribbleIDs []string
	for _, targetUserID := range subList {
		tribKey := targetUserID + ":T"
		tribbleIDs, err := ts.ls.GetList(tribKey)
		if err == nil && len(tribbleIDs)>0{
			allTribbleIDs = append(allTribbleIDs, tribbleIDs[:]...)
		}
	}
	sort.Sort(stringarr(allTribbleIDs))
	length := 100
	if len(allTribbleIDs) < 100 {
		length = len(allTribbleIDs)
	}
	i := 0
	tribbles := make([]tribrpc.Tribble, length)
	var tribble tribrpc.Tribble
	for i < length {
		mTribble, _ := ts.ls.Get(allTribbleIDs[i])
		json.Unmarshal([]byte(mTribble), &tribble)
		tribbles[i] = tribble
		i++
	}
	reply.Status = tribrpc.OK
	reply.Tribbles = tribbles
	return nil
}
