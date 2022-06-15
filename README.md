## compare tool usage

db compare "startBlockNumber" "onlyCheckAncient"


example:

nohup ./compare-tool  db compare 0 false --datadir ./data-seed  --remotedb.addrs 10.90.41.68:9239,10.90.42.15:9239,10.90.41.180:9239,
10.90.42.151:9239 > compare.log &
 

