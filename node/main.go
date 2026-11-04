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
)

type node struct {
	pb.UnimplementedRicartServer
	peers map[string]pb.RicartClient
	ip    string

	mu       sync.Mutex
	procesId int
	state    string
	lamport  int64
	deffered *list.List
}

func newNode(ip string, procesId int) *node {
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
	procesId := flag.Int("procesId", -1, "The id of the process")
	flag.Parse()
	if *procesId == -1 {
		os.Exit(1)
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
	if scanner.Err() != nil {
		log.Fatal("Error in scanning ps.txt %v", scanner.Err().Error())
	}

	count := 0
	ip := ""
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if count == *procesId {
			ip = line
			continue
		}
		if line == "" {
			continue
		}
		addrs = append(addrs, line)
	}
	if ip == "" {
		os.Exit(1)
	}

	//Starting the server
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		log.Fatalf("Fail to listen %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterRicartServer(grpcServer)
}

/*
func (n *node) Enter(ctx context.Context, pbreq *pb.Req) (*pb.Resp, error) {
	n.state = "WANTED"

	//MEtode der siger til alle jeg vil have nøglen

	//Wait statement der venter på svar fra n-1 processes
}
*/

func (s *node) Recieve(ctx context.Context)

func (s *node) Exit(ctx context.Context)
