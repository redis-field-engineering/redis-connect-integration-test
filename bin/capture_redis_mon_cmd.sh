#!/bin/bash

host="$1"
port="$2"
user="$3"
pass="$4"
[[ -z "$host" ]] && { echo "Error: Missing redis host e.g. 127.0.0.1"; exit 1; }
[[ -z "$port" ]] && { echo "Error: Missing redis port e.g. 14000"; exit 1; }

redis-cli -h ${host} -p ${port} --user ${user-default} --pass ${pass-default} monitor | while read line; do echo $line | egrep -v "CLIENT|ZRANGE|TTL|MEMORY|ZCARD|ZCOUNT|ZRANGEBYSCORE|HLEN|COMMAND|AUTH|monitor|OK|HELLO|PING|EXISTS|HGETALL|SCAN|TYPE|INFO|flushdb|pos|row|event|snapshot|last_snapshot_record|lsn|ts_usec|lsn_proc|txId" | awk '{for(i=4;i<=NF;i++){printf "%s ", $i}; printf "\n"}' >> redis-${port}_$(date +"%m%d%Y").log; done
