# Project2

**Team Members**:
Roukna Sengupta (UFID - 4947 4474), Anuja Salunkhe (UFID - 3213 0171)

**What is working:***: Gossip protocol:
In gossip protocol, we have maintained a main process which spawn a specific number of actors as specified by the user. The main process initiates the gossip by transmitting the rumour to one random node. Each node on receiving the rumour, transmits the rumour to one random neighbour. Also, at the same time, each node transmits the rumour periodically to a random neighbour to ensure that the rumour is propagated throughout the network. Thus, in case of failure of one route, the rumour can take up another route and reach other nodes in the network. We are converging the algorithm when 75% of the actors terminates.

We ran the Gossip protocol on four different network topologies – Full, line, 2D and Imperfect 2D varying the number of nodes. Below are the results we have obtained running against 50, 100, 500, 1000 and 2000 actors:

| ﻿  Gossip    | 50    | 100    | 500     | 1000     | 2000      |
|--------------|-------|--------|---------|----------|-----------|
| Full         | 607ms | 1157ms | 6328ms  | 19118ms  | 59417ms   |
| Line         | 690ms | 2713ms | 55855ms | 245615ms | 1455108ms |
| 2D           | 638ms | 1425ms | 8598ms  | 24079ms  | 90088ms   |
| Imperfect 2D | 647ms | 1114ms | 5810ms  | 24413ms  | 77683ms   |

Push Sum protocol: 
In push sum protocol, the main process spawns a specific number of actors as specified by the user. The main process initiates the transmission by forwarding the initial [s, w] value to a random actor. This actor then sends the new [s, w] value (i.e., [s/2, w/2]) to a random neighbour and keeps the other half to itself. When an actor ratio (s/w) does not change more than 10-10 for three consecutive rounds, the actor terminates. Once an actor terminates, any [s, w] sent to it from its neighbouring nodes, the actor simply forwards the value to its neighbour without performing any processing on it (i.e., not halving the value). The protocol converges when 75% of the actors terminates. 

We ran the Gossip protocol on four different network topologies – Full, line, 2D and Imperfect 2D varying the number of nodes. Below are the results we have obtained running against 50, 100, 500, 1000 and 2000 actors:

| ﻿Push sum    | 50   | 100   | 500    | 1000    | 2000    | 3000    | 4000    | 5000     | 10000    |
|--------------|------|-------|--------|---------|---------|---------|---------|----------|----------|
| Full         | 23ms | 62ms  | 1226ms | 4263ms  | 16797ms | 36625ms | 68917ms | 104587ms | 434729ms |
| Line         | 26ms | 141ms | 1683ms | 26235ms | 34778ms | 70500ms |         |          |          |
| 2D           | 25ms | 39ms  | 284ms  | 977ms   | 3569ms  | 8485ms  | 14397ms | 21526ms  | 85023ms  |
| Imperfect 2D | 36ms | 89ms  | 1545ms | 6086ms  | 22725ms | 49049ms | 88962ms | 138120ms | 624642ms |


For Gossip protocol, we can see full topology leads to earlier convergence followed by Imperfect 2D and 2D. Line topology takes more time to converge. The time taken to converge for line increases drastically with the number of actors.

For Push sum protocol, we can see that Imperfect 2D gives the best performance overall followed by 2D topology. Full topology performs well on small number of nodes but performance degrades as the number of nodes increases. The time taken to converge for line increases drastically with the number of actors.

**What is the largest network you managed to deal with for each type of topology and algorithm:**

The maximum number of actors against which we could run our Gossip protocol was 2000 actors for Full, 2D, Imperfect 2D and line topology.

The maximum number of actors against which we could run our Push sum protocol was 10000 actors for Full, 2D, Imperfect 2D topology. We could run our Push Sum protocol for a maximum of 3000 actors against line topology.


