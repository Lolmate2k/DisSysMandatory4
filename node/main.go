package main

import (
	"bufio"
	"container/list"
	"context"
	"flag"
	"fmt"
	pb "handin4/grpc"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type node struct {
	pb.UnimplementedRicartServer
	peers map[string]pb.RicartClient
	ip    string

	mu                 sync.Mutex
	procesId           int64
	state              string
	lamport            int64
	requestLamportTime int64
	deffered           *list.List
}

func newNode(ip string, procesId int64) *node {
	return &node{
		peers: make(map[string]pb.RicartClient),
		ip:    ip,

		procesId: procesId,
		state:    "RELEASED",
		lamport:  0,
		deffered: list.New(),
	}
}
func main() {
	procesId := flag.Int64("procesId", -1, "The id of the process")
	flag.Parse()
	if *procesId == -1 {
		log.Fatal("Could not start node: Invalid process ID")
	}

	// Setup logging to a log file
	logFile, err := os.OpenFile(fmt.Sprintf("logs/client_%d.log", *procesId), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("failed to open log file: %v", err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)

	// Opening of peerfile
	peerFile, err := os.Open("ps.txt")
	if err != nil {
		log.Fatalf("[Node %d] event=PEER_FILE_OPEN_FAILED err=%v", *procesId, err)
	}
	defer peerFile.Close()

	//Reading from peerfile
	var addrs []string
	scanner := bufio.NewScanner(peerFile)

	//Iterating the peerfile assigning an id and not adding ones own to the list
	count := int64(0)
	ip := ""
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if count == *procesId {
			ip = line
		} else {
			addrs = append(addrs, line)
		}
		count++
	}

	if scanner.Err() != nil {
		log.Fatalf("[Node %d] event=PEER_FILE_SCAN_FAILED err=%v", *procesId, scanner.Err())
	}
	if ip == "" {
		log.Fatalf("[Node %d] event=INVALID_PROCESS_ID reason=out_of_range", *procesId)
	}

	//Starting the server
	lis, err := net.Listen("tcp", fmt.Sprint(ip))
	if err != nil {
		log.Fatalf("[Node %d] event=LISTEN_FAILED addr=%s err=%v", *procesId, ip, err)
	}
	grpcServer := grpc.NewServer()
	n := newNode(ip, *procesId)
	pb.RegisterRicartServer(grpcServer, n)
	go func() { // Has to be in go func, since it would block if not
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("[Node %d] event=SERVE_FAILED err=%v", *procesId, err)
		}
	}()

	var conns []*grpc.ClientConn
	for _, addr := range addrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("[Node %d] event=CLIENT_CREATE_FAILED peer=%s err=%v", n.procesId, addr, err)
		}
		conns = append(conns, conn)
		n.peers[addr] = pb.NewRicartClient(conn)
	}
	defer func() { // Makes the connections to other clients close, upon closure of node
		for _, c := range conns {
			c.Close()
		}
	}()

	log.Printf("[Node %d] event=NODE_START addr=%s state=%s my_lamport=%d", n.procesId, n.ip, n.state, n.lamport)

	for {
		n.Enter()
		n.InCriticalSection()
		n.Exit()
	}
}

func (n *node) Enter() {

	n.mu.Lock()
	n.state = "WANTED"
	log.Printf("[Node %d] event=STATE_CHANGE state=%s my_lamport=%d", n.procesId, n.state, n.lamport)

	n.lamport++
	n.requestLamportTime = n.lamport
	log.Printf("[Node %d] event=REQUEST_CREATED my_lamport=%d", n.procesId, n.lamport)

	// The request we send
	req := &pb.Req{
		Lamport: n.lamport,
		PId:     n.procesId,
	}
	n.mu.Unlock()

	var wg sync.WaitGroup

	//To each other process tell i want access to the critical section
	for addr, client := range n.peers {
		wg.Add(1)
		go func() {
			log.Printf("[Node %d] event=REQUEST_SENT to=%s my_lamport=%d", n.procesId, addr, n.lamport)
			defer wg.Done()
			resp, err := client.Receive(context.Background(), req, grpc.WaitForReady(true))
			if err != nil {
				log.Printf("[Node %d] event=REQUEST_FAILED to=%s err=%v", n.procesId, addr, err)
				return
			}
			n.mu.Lock()
			log.Printf("[Node %d] event=REPLY_RECEIVED from=%s their_lamport=%d", n.procesId, addr, resp.Lamport)
			n.lamport = max(n.lamport, resp.Lamport) + 1
			n.mu.Unlock()
		}()
	}
	//Wait statement that waits for answer from N-1 processes
	wg.Wait()
	log.Printf("[Node %d] event=ALL_REPLIES_RECEIVED my_lamport=%d", n.procesId, n.lamport)

	n.state = "HELD"
	log.Printf("[Node %d] event=STATE_CHANGE state=%s my_lamport=%d", n.procesId, n.state, n.lamport)
}

func (n *node) InCriticalSection() {
	log.Printf("[Node %d] event=CS_ENTER my_lamport=%d", n.procesId, n.lamport)

	fmt.Printf("%d accessing the mainframe\n", n.procesId)
	fmt.Printf("%d is bypassing the firewall\n", n.procesId)
	time.Sleep(time.Second)
	fmt.Printf("%d I'm in!\n", n.procesId)

	log.Printf("[Node %d] event=CS_EXIT my_lamport=%d", n.procesId, n.lamport)
}

func (n *node) Receive(ctx context.Context, pbreq *pb.Req) (*pb.Resp, error) {
	n.mu.Lock()
	n.lamport = max(n.lamport, pbreq.Lamport) + 1
	log.Printf("[Node %d] event=REQUEST_RECEIVED from=%d their_lamport=%d my_state=%s my_lamport=%d", n.procesId, pbreq.PId, pbreq.Lamport, n.state, n.lamport)
	if n.state == "HELD" || (n.state == "WANTED" && n.isItMyTurn(pbreq)) {
		reason := "WANTED_WITH_PRIORITY"
		if (n.state == "HELD") {reason = "HELD"}
		log.Printf("[Node %d] event=REQUEST_DEFERRED from=%d reason=%s my_lamport=%d", n.procesId, pbreq.PId, reason, n.lamport)
		wait := make(chan struct{})
		n.deffered.PushBack(wait)
		n.mu.Unlock()
		<-wait
		log.Printf("[Node %d] event=DEFERRED_RELEASED to=%d my_lamport=%d", n.procesId, pbreq.PId, n.lamport)
		n.mu.Lock()
	}
	n.lamport++
	tmpLamport := n.lamport // saved for operations outside mutex, since we are not 100% sure that the lamport clock will not update before the logprint and return
	n.mu.Unlock()

	log.Printf("[Node %d] event=REPLY_SENT to=%d my_lamport=%d", n.procesId, pbreq.PId, tmpLamport)
	return &pb.Resp{Lamport: tmpLamport}, nil
}

func (n *node) isItMyTurn(pbreq *pb.Req) bool {
	if n.requestLamportTime < pbreq.Lamport {
		return true
	} else if n.requestLamportTime > pbreq.Lamport {
		return false
	}

	if n.procesId <= pbreq.PId {
		return true
	}
	return false
}

func (n *node) Exit() {
	n.state = "RELEASED"
	log.Printf("[Node %d] event=STATE_CHANGE state=%s my_lamport=%d", n.procesId, n.state, n.lamport)
	log.Printf("[Node %d] event=RELEASING_DEFERRED count=%d", n.procesId, n.deffered.Len())
	for range n.deffered.Len() {
		waitingNode := n.deffered.Front()
		close(waitingNode.Value.(chan struct{}))
		n.deffered.Remove(waitingNode)
	}

}
