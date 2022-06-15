

# migrate process:

main process scans data, dispatcher distributes tasks, and records the number of tasks, the work pool automatically schedules tasks, and records the number of completed tasks. 

Before the command exits, wait for the number of completed tasks in the work pool to reach the number of distributed tasks, and then close the work pool.

# usage:

Use the command:
 ./ geth db migrate  <ancient block start number> <only migrate ancient data> <kvocksdb addr>
 



Example:
  nohup  ./geth  db migrate 0 false --datadir ./node --remotedb.addrs 127.0.0.1:6668,127.0.0.1:6669 &

