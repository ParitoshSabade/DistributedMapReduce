#!/bin/bash
set -x
nohup python3 database_server.py $1 </dev/null 2>&1 >>database_log &
set +x