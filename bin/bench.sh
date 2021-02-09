#!/bin/bash

export SPARK_GENERATE_BENCHMARK_FILES=1
mkdir -p benchmarks/

SPARK_HOME={{PATH TO SPARK BINARY}}
ARROW_HOME={{PATH TO ARROW BINARY}}

if [ -z "${SPARK_HOME}" ]; then
  echo "env SPARK_HOME not defined" 1>&2
  exit 1
fi
export LIBARROW_DIR=$ARROW_HOME
SCRIPT_DIR=$(cd `dirname $0` && pwd)
BENCH_JAR=$SCRIPT_DIR/../target/native-sql-engine-benchmark-0.1.0-SNAPSHOT-jar-with-dependencies.jar

BATCH_SIZE=10240
MALLOC_ARENAS=4

echo "Using \`spark-submit\` from path: $SPARK_HOME" 1>&2
exec "${SPARK_HOME}"/bin/spark-submit \
  --name tpcds-hongze \
  --master local[*] \
  --num-executors 1 \
  --driver-memory 3g \
  --executor-memory 6g \
  --executor-cores 4 \
  --conf spark.sql.files.maxPartitionBytes=384MB \
  --conf spark.sql.shuffle.partitions=288 \
  --conf spark.executor.extraJavaOptions="-XX:MaxDirectMemorySize=16g -Dio.netty.allocator.numDirectArena=${MALLOC_ARENAS}" \
  --conf spark.executor.memoryOverhead=5g \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=16g \
  --conf spark.sql.extensions=com.intel.oap.ColumnarPlugin \
  --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager \
  --conf spark.sql.columnar.codegen.hashAggregate=false \
  --conf spark.sql.columnar.sort=true \
  --conf spark.oap.sql.columnar.sortmergejoin=true \
  --conf spark.sql.join.preferSortMergeJoin=false \
  --conf spark.sql.columnar.sort.broadcastJoin=true \
  --conf spark.sql.inMemoryColumnarStorage.batchSize=${BATCH_SIZE} \
  --conf spark.sql.parquet.columnarReaderBatchSize=${BATCH_SIZE} \
  --conf spark.sql.execution.arrow.maxRecordsPerBatch=${BATCH_SIZE} \
  --conf spark.oap.sql.columnar.preferColumnar=true \
  --conf spark.oap.sql.columnar.wholestagecodegen=true \
  --conf spark.oap.sql.columnar.hashCompare=true \
  --conf spark.oap.sql.columnar.numaBinding=false \
  --conf spark.oap.sql.columnar.wholestagecodegen.breakdownTime=false \
  --conf "spark.oap.sql.columnar.coreRange=0-25,52-77|26-51,78-104" \
  --conf spark.driver.extraClassPath=$BENCH_JAR \
  --conf spark.executor.extraClassPath=$BENCH_JAR \
  --conf spark.executorEnv.LIBARROW_DIR=$ARROW_HOME \
  --conf spark.executorEnv.MALLOC_ARENA_MAX=$MALLOC_ARENAS \
  --conf spark.executorEnv.MALLOC_CONF=narenas:$MALLOC_ARENAS \
  --conf spark.sql.sources.useV1SourceList=arrow,parquet \
  --class org.apache.spark.nsebench.TPCDSNative \
  $BENCH_JAR \
  "$@"
