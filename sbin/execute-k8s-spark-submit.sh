#!/bin/bash
# Kubernetes Spark Submit Script
# Usage: ./execute-k8s-spark-submit.sh [APP_NAME] [MAIN_CLASS] [JAR_PATH] [EXTRA_ARGS]

# Required parameters
APP_NAME=$1
MAIN_CLASS=$2
JAR_PATH=$3
EXTRA_ARGS=$4

# Kubernetes configuration
K8S_MASTER="k8s://https://kubernetes.default.svc"
DRIVER_MEMORY="2g"
EXECUTOR_MEMORY="4g"
EXECUTOR_CORES=2
NUM_EXECUTORS=3

# Spark submit command
spark-submit \
  --master $K8S_MASTER \
  --deploy-mode cluster \
  --name $APP_NAME \
  --class $MAIN_CLASS \
  --conf spark.kubernetes.container.image=apache/spark:v3.3.0 \
  --conf spark.kubernetes.driver.pod.name=$APP_NAME-driver \
  --conf spark.kubernetes.executor.podNamePrefix=$APP_NAME-exec \
  --conf spark.executor.instances=$NUM_EXECUTORS \
  --conf spark.driver.memory=$DRIVER_MEMORY \
  --conf spark.executor.memory=$EXECUTOR_MEMORY \
  --conf spark.executor.cores=$EXECUTOR_CORES \
  $EXTRA_ARGS \
  $JAR_PATH

# Exit with spark-submit status
exit $?
