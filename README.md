# DisSysMandatory4

To start a node, make sure you are in the 'node' directory and then do the following:
```bash
go run ./main.go --procesId <ID>
```
Where \<ID> is an int from 0-2. The code right now only supports 3 nodes.
If more nodes are wanted, find 'ps.txt' and add new ip's on the lines underneath the ones already there.
Then just start the new amount of nodes, corrosponding the amount of ip's added. Make sure to give each node another Id

==IMPORTANT==
==Nodes will only gain access when all other nodes are live, so make sure all possible nodes are running.==

To see the logs, one must look at the files in the 'logs' directory.