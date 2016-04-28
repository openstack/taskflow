#!/bin/bash

# This requires https://pypi.python.org/pypi/zk_shell/ to be installed...

set -e

ZK_HOSTS=${ZK_HOSTS:-localhost:2181}
TF_PATH=${TF_PATH:-taskflow}

for path in `zk-shell --run-once "ls" $ZK_HOSTS`; do
    if [[ $path == ${TF_PATH}* ]]; then
        echo "Removing (recursively) path \"$path\""
        zk-shell --run-once "rmr $path" $ZK_HOSTS
    fi
done
