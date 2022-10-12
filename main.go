package main

import (
	"encoding/json"
	"fmt"
	"github.com/emirpasic/gods/utils"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/gin-gonic/gin"
)

type Data map[string]string

type Command map[string]string

type Server struct {
	InitialState Data
	CurrentState Data
	Diff         treemap.Map
	RemoteDiff   treemap.Map
	Port         int
	LastReceived int64
	FriendList   []string
	Alive        bool
	Lock         sync.Mutex
}

func (server *Server) merge() {
	// merge remote diff with current diff
	// update the current state
	// update current diff
	// remove remote diff
	// flush
	server.Alive = false
	i, j := 0, 0
	server.Lock.Lock()
	defer server.Lock.Unlock()
	hasIds := server.Diff.Keys()
	idLen := len(server.Diff.Keys())
	outIds := server.RemoteDiff.Keys()
	outLen := len(server.RemoteDiff.Keys())
	for i < idLen && j < outLen {
		// remove common diff
		//println(i, server.Diff.Size(), j, server.RemoteDiff.Size())
		currDiffId, _ := strconv.Atoi(fmt.Sprintf("%d", hasIds[i]))
		currRemoteDiffId, _ := strconv.Atoi(fmt.Sprintf("%d", outIds[j]))
		if currDiffId == currRemoteDiffId {
			hasVal, _ := server.Diff.Get(int64(currDiffId))
			outVal, _ := server.RemoteDiff.Get(int64(currRemoteDiffId))
			if fmt.Sprintf("%v", hasVal) == fmt.Sprintf("%v", outVal) {
				i += 1
				j += 1
			} else {
				//  (tiebreaker use LWW )
				// reject remote write and continue
				i += 1
				j += 1
			}
		} else if currDiffId > currRemoteDiffId {
			outVal, _ := server.RemoteDiff.Get(int64(currRemoteDiffId))
			server.Diff.Put(int64(currRemoteDiffId), outVal)
			j += 1
		} else {
			i += 1
		}
	}

	server.RemoteDiff.Clear()
	server.CurrentState = make(Data, 0)
	it := server.Diff.Iterator()
	for it.End(); it.Prev(); {
		value := it.Value()
		v, _ := value.(map[string]string)
		for key, valx := range v {
			val1, has := server.CurrentState[key]
			if !has {
				server.CurrentState[key] = valx
				continue
			}
			curr, err := strconv.Atoi(val1)
			if err != nil {
				continue
			}
			change, err := strconv.Atoi(valx)
			if err != nil {
				continue
			}
			newData := curr + change
			server.CurrentState[key] = strconv.Itoa(newData)
		}
	}
	server.Alive = true
}

func NewServer(port int, initialState Data, friendList []string) *Server {
	return &Server{
		InitialState: initialState,
		CurrentState: initialState,
		Diff:         *treemap.NewWith(utils.Int64Comparator),
		RemoteDiff:   *treemap.NewWith(utils.Int64Comparator),
		Port:         port,
		LastReceived: 0,
		Alive:        true,
		FriendList:   friendList,
	}
}

func Ping(server *Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		server.Lock.Lock()
		defer server.Lock.Unlock()
		if server.Alive {
			c.String(200, "Pong")
			return
		} else {
			c.String(502, "Unreachable")
			return
		}
	}
}

func GetState(server *Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		if server.Alive {
			c.JSON(200, server.CurrentState)
			return
		} else {
			c.String(502, "Unreachable")
			return
		}
	}
}

func AliveState(server *Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		server.Lock.Lock()
		defer server.Lock.Unlock()
		alive, err := strconv.ParseBool(c.Param("alive_status"))
		if err != nil {
			c.String(500, err.Error())
			return
		}
		server.Alive = alive
	}
}

func Gossip(server *Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		server.Lock.Lock()
		defer server.Lock.Unlock()
		if server.Alive {
			data, err := server.Diff.ToJSON()
			if err != nil {
				c.String(500, err.Error())
			}
			c.Header("Content-Type", "application/json")
			c.String(200, string(data))
			return
		} else {
			c.String(502, "Unreachable")
			return
		}
	}
}

func AddCommand(server *Server) gin.HandlerFunc {
	return func(c *gin.Context) {
		server.Lock.Lock()
		defer server.Lock.Unlock()
		if server.Alive {
			body, err := ioutil.ReadAll(c.Request.Body)
			if err != nil {
				c.String(500, "Request body is invalid")
			}
			var data Command
			err = json.Unmarshal(body, &data)
			if err != nil {
				c.String(500, "Request body is invalid")
			}
			server.Diff.Put(time.Now().UnixMilli(), &data)
			for key, value := range data {
				val1, has := server.CurrentState[key]
				if !has {
					server.CurrentState[key] = value
					c.String(200, "Inserted")
					return
				}
				curr, err := strconv.Atoi(val1)
				if err != nil {
					c.String(500, err.Error())
					return
				}
				change, err := strconv.Atoi(value)
				if err != nil {
					c.String(500, err.Error())
					return
				}
				newData := curr + change
				server.CurrentState[key] = strconv.Itoa(newData)
			}
			c.String(200, "Inserted")
			return
		} else {
			c.String(502, "Unreachable")
			return
		}
	}
}

func createServer(port int) {
	initialState := map[string]string{}
	friendList := make([]string, 0)
	for i := 8080; i < 8090; i++ {
		friendList = append(friendList, "http://localhost:"+strconv.Itoa(i))
	}
	server := NewServer(port, initialState, friendList)
	fmt.Println("Starting server at port " + strconv.Itoa(server.Port))
	router := gin.Default()
	go func() {
		rand.Seed(time.Now().Unix())
		for {
			time.Sleep(1500 * time.Millisecond)
			friendToAskGossip := server.FriendList[rand.Intn(len(server.FriendList))]
			req, err := http.NewRequest(http.MethodGet, friendToAskGossip+"/gossip", nil)
			if err != nil {
				continue
			}
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				continue
			}
			if res.StatusCode < 399 {

				data, err := ioutil.ReadAll(res.Body)
				if err != nil {
					continue
				}
				var remoteDiff map[string]map[string]string
				err = json.Unmarshal(data, &remoteDiff)
				if err != nil {
					continue
				}
				for key, value := range remoteDiff {
					atoi, err := strconv.Atoi(key)
					if err != nil {
						return
					}
					server.RemoteDiff.Put(int64(atoi), value)
				}
				server.merge()
			}

		}
	}()
	router.GET("/gossip", Gossip(server))
	router.GET("/ping", Ping(server))
	router.GET("/data", GetState(server))
	router.POST("/data", AddCommand(server))
	router.GET("/condition", AliveState(server))
	err := router.Run("localhost:" + strconv.Itoa(port))
	if err != nil {
		return
	}
}

func dummyInsertions(urls ...string) {
	allowedChangeKeys := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
	minChange := -10
	maxChange := 10
	client := &http.Client{}
	defer client.CloseIdleConnections()
	for {
		time.Sleep(300 * time.Millisecond)
		key := allowedChangeKeys[rand.Intn(len(allowedChangeKeys))]
		val := rand.Intn(maxChange) + 2*minChange
		toHit := urls[rand.Intn(len(urls))]
		data, err := json.Marshal(map[string]string{
			string(key): strconv.Itoa(val),
		})
		if err != nil {
			continue
		}
		payload := strings.NewReader(string(data))

		req, err := http.NewRequest("POST", toHit, payload)

		if err != nil {
			fmt.Println(err)
			continue
		}
		req.Header.Add("Content-Type", "application/json")

		res, err := client.Do(req)
		if err != nil {
			fmt.Println(err)
			continue
		}

		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			fmt.Println(err)
			continue
		}
		fmt.Println(string(body))
	}

}

func main() {
	wait := make(chan bool, 1)
	urls := make([]string, 0)
	for i := 8080; i < 8085; i++ {
		time.Sleep(300 * time.Millisecond)
		go createServer(i)
		urls = append(urls, "http://localhost:"+strconv.Itoa(i)+"/data")
	}
	go dummyInsertions(urls...)
	t := <-wait
	fmt.Println(t)
}
