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

	// Opening of peerfile
	peerFile, err := os.Open("ps.txt")
	if err != nil {
		log.Fatalf("Could not open peer file %v", err)
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
		log.Fatalf("Error in scanning ps.txt %v", scanner.Err().Error())
	}
	if ip == "" {
		log.Fatal("Could not start node: Proces id was out of range")
	}

	//Starting the server
	lis, err := net.Listen("tcp", fmt.Sprint(ip))
	if err != nil {
		log.Fatalf("Fail to listen %v", err)
	}
	grpcServer := grpc.NewServer()
	n := newNode(ip, *procesId)
	pb.RegisterRicartServer(grpcServer, n)
	go func() { // Has to be in go func, since it would block if not
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve : %v", err)
		}
	}()

	var conns []*grpc.ClientConn
	for _, addr := range addrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("%d Could not create client for %s: %v", n.procesId, addr, err)
		}
		conns = append(conns, conn)
		n.peers[addr] = pb.NewRicartClient(conn)
	}
	defer func() { // Makes the connections to other clients close, upon closure of node
		for _, c := range conns {
			c.Close()
		}
	}()

	log.Printf("%d: State changed to %s", n.procesId, n.state)

	for {
		n.Enter()
		n.InCriticalSection()
		n.Exit()
	}
}

func (n *node) Enter() {

	n.mu.Lock()
	n.state = "WANTED"
	log.Printf("%d: State changed to %s", n.procesId, n.state)

	n.lamport++
	n.requestLamportTime = n.lamport

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
			defer wg.Done()
			resp, err := client.Receive(context.Background(), req, grpc.WaitForReady(true))
			if err != nil {
				log.Fatalf("Request to %s failed: %v", addr, err)
			}

			n.mu.Lock()
			n.lamport = max(n.lamport, resp.Lamport) + 1
			n.mu.Unlock()
		}()
	}
	//Wait statement that waits for answer from N-1 processes
	wg.Wait()

	n.state = "HELD"
	log.Printf("%d: State changed to %s", n.procesId, n.state)
}

func (n *node) InCriticalSection() {
	log.Printf("%d entered the critical section", n.procesId)

	fmt.Printf("%d accessing the mainframe", n.procesId)
	fmt.Printf("%d is bypassing the firewall", n.procesId)
	fmt.Printf("%d I'm in!", n.procesId)

	log.Printf("%d finished working in the critical section", n.procesId)
}

func (n *node) Receive(ctx context.Context, pbreq *pb.Req) (*pb.Resp, error) {
	n.mu.Lock()
	n.lamport = max(n.lamport, pbreq.Lamport) + 1
	if n.state == "HELD" || (n.state == "WANTED" && n.isItMyTurn(pbreq)) {
		wait := make(chan struct{})
		n.deffered.PushBack(wait)
		n.mu.Unlock()
		<-wait
		n.mu.Lock()
	}
	n.lamport++
	n.mu.Unlock()

	return &pb.Resp{Lamport: n.lamport}, nil
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
	log.Printf("%d: State changed to %s", n.procesId, n.state)

	for range n.deffered.Len() {
		waitingNode := n.deffered.Front()
		close(waitingNode.Value.(chan struct{}))
		n.deffered.Remove(waitingNode)
	}

}
