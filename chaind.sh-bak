#!/bin/bash
  
export GOGC=100
function startChaind() {
    workspace=/server/data-seed
    ethstats_endpoint=$(hostname):MJCijac0upoG@ethstats.dex.internal
    METRICS_MP_METRICS_ENABLED=true ${workspace}/bsc_perf --syncmode full --pipecommit --pprof --pprof.addr 0.0.0.0 --rpc.allow-unprotected-txs --metrics  --cache 5000 --txlookuplimit  0  --ws.port 8545  --ws.addr 0.0.0.0  --light.serve 50 --ws --datadir ${workspace}/  --http.corsdomain "*" --config ${workspace}/config.toml >> /server/logs/bscnode.log 2>&1
}
function stopChaind() {
    pid=`ps -ef | grep /server/data-seed/bsc | grep -v grep | awk '{print $2}'`
    if [ -n "$pid" ]; then
        kill -TERM $pid
        for((i=1;i<=40;i++));
        do
            pid=`ps -ef | grep /server/data-seed/bsc | grep -v grep | awk '{print $2}'`
            if [ -z "$pid" ]; then
                break
            fi
            sleep 10
        done
    fi
}
CMD=$1
case $CMD in
-start)
    echo "start"
    startChaind
    ;;
-stop)
    echo "stop"
    stopChaind
    ;;
-restart)
    stopChaind
    sleep 3
    startChaind
    ;;
*)
    echo "Usage: chaind.sh -start | -stop | -restart .Or use systemctl start | stop | restart bsc.service "
    ;;
esac
