#!/bin/bash

set -e

TIMEOUT_SECONDS=60

META_DB_PATH=${META_DB_PATH}
PERFORMANCE_DB_PATH=${PERFORMANCE_DB_PATH}
DATA_PATH=${DATA_PATH}
LOG_PATH=${LOG_PATH}
NO_OF_WORKERS=${NO_OF_WORKERS}
ENABLE_NMON=${ENABLE_NMON}

while [ $# -gt 0 ]; do

    if [[ $1 == *"--"* ]]; then
        param="${1/--/}"
        declare $param="$2"
        echo $1 "=" $2
    fi

    shift
done

if [ -z "$META_DB_PATH" ]; then
    echo "META_DB_PATH SHOULD BE SPECIFIED"
    exit 1
fi

if [ -z "$PERFORMANCE_DB_PATH" ]; then
    echo "PERFORMANCE_DB_PATH SHOULD BE SPECIFIED"
    exit 1
fi

if [ -z "$DATA_PATH" ]; then
    echo "DATA_PATH SHOULD BE SPECIFIED"
    exit 1
fi

if [ -z "$LOG_PATH" ]; then
    echo "LOG_PATH SHOULD BE SPECIFIED"
    exit 1
fi

if [ -z "$NO_OF_WORKERS" ]; then
    NO_OF_WORKERS="2"
fi

if [ -z "$ENABLE_NMON" ]; then
    ENABLE_NMON="false"
fi

kubectl apply -f ./k8s/rbac.yaml
kubectl apply -f ./k8s/pushgateway.yaml

# wait until pushgateway starts listening
cur_timestamp="$(date +%s)"
end_timestamp="$((cur_timestamp + TIMEOUT_SECONDS))"
spin="/-\|"
i=0
while true; do
    if [ "$(date +%s)" -gt "$end_timestamp" ]; then
        echo "Timeout"
        exit 1
    fi
    pushgatewayIP="$(kubectl get services |& grep pushgateway | tr '\t' ' ' | tr -s ' ' | cut -d ' ' -f 3)"
    if [ ! -z "$pushgatewayIP" ]; then
        break
    fi
    printf "Waiting pushgateway to start [%c] \r" "${spin:i++%${#spin}:1}"
    sleep .2
done

pushgateway_address="${pushgatewayIP}:9091" envsubst <"./k8s/prometheus.yaml" | kubectl apply -f -
sed -i "s#org.jasminegraph.collector.pushgateway=.*#org.jasminegraph.collector.pushgateway=http://${pushgatewayIP}:9091/metrics/job/#" ./conf/jasminegraph-server.properties

docker build -t jasminegraph .

metadb_path="${META_DB_PATH}" \
    performancedb_path="${PERFORMANCE_DB_PATH}" \
    data_path="${DATA_PATH}" \
    log_path="${LOG_PATH}" \
    envsubst <"./k8s/volumes.yaml" | kubectl apply -f -

no_of_workers="${NO_OF_WORKERS}" \
    enable_nmon="${ENABLE_NMON}" \
    envsubst <"./k8s/master-deployment.yaml" | kubectl apply -f -

# wait until master starts listening
cur_timestamp="$(date +%s)"
end_timestamp="$((cur_timestamp + TIMEOUT_SECONDS))"
spin="/-\|"
i=0
while true; do
    if [ "$(date +%s)" -gt "$end_timestamp" ]; then
        echo "Timeout"
        exit 1
    fi
    masterIP="$(kubectl get services |& grep jasminegraph-master-service | tr '\t' ' ' | tr -s ' ' | cut -d ' ' -f 3)"
    if [ ! -z "$masterIP" ]; then
        echo
        echo "Connect to JasmineGraph at $masterIP:7777"
        echo
        break
    fi
    printf "Waiting JasmineGraph to start [%c] \r" "${spin:i++%${#spin}:1}"
    sleep .2
done
