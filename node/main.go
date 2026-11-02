package main

import (
	"bufio"
	"container/list"
	"context"
	"flag"
	pb "handin4/grpc"
	"log"
	"os"
	"strings"
	"sync"
)

type node struct {
	pb.UnimplementedRicartServer
	peers map[string]pb.RicartClient
	ip    string

	mu       sync.Mutex
	procesId string
	state    string
	lamport  int
	deffered *list.List
}

func main() {
	procesId := flag.String("procesId", "", "The id of the process")
	flag.Parse()
	if *procesId == "" {
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
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		addrs = append(addrs, line)
	}
}

func (s *node) Enter(ctx context.Context)
