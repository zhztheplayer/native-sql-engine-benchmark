#!/bin/bash

export SPARK_GENERATE_BENCHMARK_FILES=1
mkdir -p benchmarks/

SPARK_HOME={{PATH TO SPARK BINARY}}

if [ -z "${SPARK_HOME}" ]; then
  echo "env SPARK_HOME not defined" 1>&2
  exit 1
fi
SCRIPT_DIR=$(cd `dirname $0` && pwd)
BENCH_JAR=$SCRIPT_DIR/../target/native-sql-engine-benchmark-0.1.0-SNAPSHOT-jar-with-dependencies.jar

BATCH_SIZE=10240

echo "Using \`spark-submit\` from path: $SPARK_HOME" 1>&2
exec "${SPARK_HOME}"/bin/spark-submit \
  --name tpcds-hongze \
  --master local[*] \
  --num-executors 1 \
  --driver-memory 3g \
  --executor-memory 16g \
  --executor-cores 4 \
  --conf spark.sql.files.maxPartitionBytes=384MB \
  --conf spark.sql.shuffle.partitions=288 \
  --conf spark.executor.extraJavaOptions="-XX:MaxDirectMemorySize=6g" \
  --conf spark.executor.memoryOverhead=5g \
  --conf spark.memory.offHeap.enabled=false \
  --conf spark.sql.join.preferSortMergeJoin=false \
  --conf spark.sql.parquet.columnarReaderBatchSize=${BATCH_SIZE} \
  --conf spark.driver.extraClassPath=$BENCH_JAR \
  --conf spark.executor.extraClassPath=$BENCH_JAR \
  --class org.apache.spark.nsebench.TPCDSVanilla \
  $BENCH_JAR \
  --use-parquet-format \
  "$@"
