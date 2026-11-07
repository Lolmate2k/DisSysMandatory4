# Game plan: Ricart-Agrawala handin

Status as of 2026-11-07 (updated after items 1-3, 6, 8, 10-13). Read from `node/main.go`, `grpc/handin4.proto` and `ps.txt`. `go build` and `go vet` pass. Nothing has been run yet, so "works" below means "reads correctly", not "tested". Line numbers refer to the current `node/main.go` and will shift as you edit, so treat them as a guide.

## A. Foundation (done)

- [x] **Proto defined.** `Receive(Req) returns (Resp)`, with `lamport` and `pId` in `Req`. Matches the algorithm (request + deferred reply as the return value).
- [x] **gRPC code generated.** `handin4.pb.go` and `handin4_grpc.pb.go` exist.
- [x] **Go module created.** `go.mod` has `module handin4`.
- [x] **Node struct with algorithm state.** State, Lamport clock, own request time, deferred queue (`container/list`), mutex.
- [x] **Receive handler written.** Updates the clock, defers when `HELD`, or when `WANTED` and this node has priority.
- [x] **Priority check written.** `isItMyTurn` compares `(lamport, id)`, lower wins.
- [x] **Enter / critical section / Exit skeletons written.** Request is sent to all peers in parallel goroutines with a `WaitGroup`.

## B. Bugs and gaps in `node/main.go` (must fix before it runs)

- [x] **1. Fix the peers-file parsing (lines ~71-84).** `count` is only incremented for peers, not for your own line. After your own line is found, `count` still equals `procesId`, so every later line also matches and overwrites `ip`. With `-procesId 0` you end up with the *last* address and no peers. Fix: use a plain line index that increments for every non-empty line, and compare that index to `procesId`.
- [x] **2. Actually create the peer clients.** `n.peers` is never filled. For each peer address: `grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))`, then `pb.NewRicartClient(conn)`, then store it in `n.peers`. Close the connections on shutdown. Needs the import `google.golang.org/grpc/credentials/insecure`.
- [x] **3. Run the server in a goroutine.** `grpcServer.Serve(lis)` blocks, so nothing after it (including your request loop) ever runs. Wrap it in `go func() { ... }()`.
- [ ] **4. Don't request the critical section in a tight infinite loop (`main`, lines ~124-129).** `for { Enter(); InCriticalSection(); Exit() }` spins forever. Trigger requests from something controllable: stdin ("press Enter to request") or a random sleep. The spec says "any node may request at any time". Make sure `main` still blocks afterwards so the server goroutine keeps running.
- [ ] **5. Lock the mutex around every read/write of the shared state outside `Receive`.** These are all still unlocked:
  - `Enter`: `n.state = "HELD"` and its `STATE_CHANGE` log (lines ~172-173), the `ALL_REPLIES_RECEIVED` log (~170).
  - The `REQUEST_SENT` log in the goroutine (line ~154) reads `n.lamport`. Use `req.Lamport` instead, you already have it.
  - `InCriticalSection`: the `CS_ENTER` and `CS_EXIT` logs read `n.lamport` (lines ~177, ~184).
  - `Exit` (lines ~224-232): the state change, the logs, and the deferred-list loop. The deferred list is modified unlocked there, which races with `Receive` (which pushes onto it under the lock).
  - `Receive` line ~199: the `DEFERRED_RELEASED` log reads `n.lamport` right after `<-wait`, before `n.mu.Lock()`. Move the log below the `Lock()`.
  - `main` line ~122: the `NODE_START` log reads `n.lamport` while the server goroutine is already running (harmless in practice, but `-race` will flag it).
- [x] **6. Don't read `n.lamport` outside the lock in `Receive`.** Done: `tmpLamport` is copied under the lock and used for both the log and the return.
- [ ] **7. Make `Exit` simpler and safe (lines ~224-232).** Under the lock: loop the deferred list, `close` each channel, `Init()` the list, set `RELEASED`. This is the same work as item 5 for `Exit`, so do them together.
- [x] **8. No more `log.Fatalf` in the request goroutine.** Done: it now logs and returns (lines ~157-160). Note that if a peer call fails, `wg.Wait()` still returns and the node would enter the critical section without that peer's reply. That is acceptable for the assignment (peers are assumed to stay up), but don't claim fault tolerance in the report.
- [ ] **9. Make the critical section last a visible amount of time (`InCriticalSection`, lines ~176-185).** Still open: add `time.Sleep(time.Second)` (plus `"time"` in the imports) between the "firewall" and "I'm in!" prints (the blank line ~181), and add `\n` to the three `Printf` lines, which currently run together on one line. Without a delay you can't demonstrate mutual exclusion in the logs. A 1 s sleep is short, so overlap between nodes is easy to miss. Consider 2 s if you can't see a deferral in your test runs.
- [x] **10. Dependencies downloaded.** `go mod download` and `go mod tidy` work (after switching `GOPROXY` to a mirror). `go build ./...` and `go vet ./...` both pass.

## C. Logging (needed for the report)

- [x] **11. One log file per node.** Done (lines ~51-57): the file is `logs/client_<id>.log`. Optional: rename it to `node_<id>.log` and add a `[Node <id>]` prefix with `log.SetPrefix`, so the appendix reads clearly. The `logs/` folder exists.
- [x] **12. Log every step of the algorithm.** Done. All steps are logged in the `[Node N] event=... key=value` style: request created / sent / received, deferred (with `reason=HELD` or `WANTED_WITH_PRIORITY`), deferred released, reply sent, reply received, all replies received, state changes, critical section enter / exit, and the number of deferred replies released on exit.
- [x] **13. Log the Lamport clock value in each line.** Done (all event lines carry `my_lamport=` or `their_lamport=`). Small cosmetic fix: the `CS_ENTER` log has a stray comma (`event=CS_ENTER, my_lamport=`), and so does `NODE_START` (`state=%s, my_lamport=`). Remove the commas so every line parses the same way.

## D. Run and test

- [ ] **14. Clean up `ps.txt`.** One `host:port` per line, no trailing spaces. `127.0.0.1:8080`, `127.0.0.2:8080`, `127.0.0.3:8080` works if your OS accepts all three loopback addresses. Safer: `localhost:8081`, `:8082`, `:8083`.
- [ ] **15. Start three nodes** in three terminals (`go run ./node -procesId 0/1/2`) and confirm they all start and connect. Your `grpc.WaitForReady(true)` on the request call already handles nodes that start in a different order.
- [ ] **16. Test a single node requesting.** The other two should reply immediately, and the requester enters the critical section.
- [ ] **17. Test two nodes requesting at once** (the interesting case). One must be deferred, and the logs must show that only one is in the critical section at a time (R2), and that the second enters after the first exits (R3).
- [ ] **18. Test all three requesting repeatedly.** Check that nobody starves and the tie-break by node id works.
- [ ] **19. Run `go vet ./...` and `go run -race`** to catch the data races from items 5 and 6.
- [ ] **20. Save the final evidence run** into `logs/` (a run showing a clear request-defer-reply-enter sequence).

## E. Repo hygiene

- [ ] **21. Delete one of the two readmes.** You have both `README.md` and `README.txt`. Keep `README.md`.
- [ ] **22. Write `README.md`.** Needs: prerequisites (Go), the format of the peers file, the exact commands to start each of the three nodes (from the project root), how to trigger a request, and where the logs go.
- [ ] **23. Add `.gitignore`** (`*.exe`) and run `gofmt -w .`.
- [ ] **24. Commit and push** so the report can link to a working repo. Don't commit this `TODO.md` unless you want to.

## F. Report (PDF, maximum 2 pages)

- [ ] **25. System description.** One short paragraph: peer-to-peer, every node is both a gRPC server and a client to all others, service discovery through `ps.txt`.
- [ ] **26. R1 (spec).** Explain the emulated critical section and that any node can request at any time.
- [ ] **27. R2 (safety).** Explain why only one node can be in the critical section (a node enters only after replies from all other nodes; replies are held back while `HELD` or when you have priority). Point at a log excerpt.
- [ ] **28. R3 (liveness).** Explain why every request is eventually granted: requests are totally ordered by `(Lamport time, id)`, and deferred replies are sent on exit. Point at a log excerpt.
- [ ] **29. Algorithm discussion using log examples.** Walk through one concrete sequence from your logs (request, deferral, reply, enter, exit).
- [ ] **30. Link to the Git repo.**
- [ ] **31. Appendix with the system logs,** within the 2-page limit (trim to the relevant excerpts, a small font is fine).
- [ ] **32. Export to PDF and re-read** against the hand-in requirements list.
