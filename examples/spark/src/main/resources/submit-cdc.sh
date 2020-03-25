#!/usr/bin/sh
source /opt/client/bigdata_env
spark-submit --master yarn --deploy-mode cluster  --driver-memory 20g --driver-cores 4 --executor-memory 50g \
--executor-cores 10  --num-executors 3 \
--conf spark.executor.memoryOverhead=10240 \
--files spark-obs.conf \
--jars carbondata-examples-2.0.0-SNAPSHOT.jar \
--class org.apache.carbondata.examples.CDCExampleOBS carbondata-examples-2.0.0-SNAPSHOT.jar \
hdfs://hacluster/carbon2.properties carbon 100000 1000 9000 1000 10
