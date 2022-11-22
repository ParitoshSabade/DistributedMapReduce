#!/bin/bash
set -x

nohup python3 $1 $2 $3 $4 &

set +x