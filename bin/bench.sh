#!/bin/bash

if [ -z "${SPARK_HOME}" ]; then
  echo "env SPARK_HOME not defined" 1>&2
  exit 1
fi

SCRIPT_DIR=$(cd `dirname $0` && pwd)
JAR=$SCRIPT_DIR/../target/native-sql-engine-benchmark-0.1.0-SNAPSHOT-jar-with-dependencies.jar

echo "Using \`spark-submit\` from path: $SPARK_HOME" 1>&2
exec "${SPARK_HOME}"/bin/spark-submit \
  --class org.apache.spark.nsebench.NSETPCDSQueryBenchmark \
  $JAR \
  "$@"
