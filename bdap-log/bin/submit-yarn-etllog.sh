JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk.x86_64/
SPARK_HOME=/data/spark-2.0.0-bin-hadoop2.7
#ENGINE_LIB=hdfs://192.85.247.104:19000/bdap-r0.4.0/engine/lib
#LOG_LIB=hdfs://192.85.247.104:19000/bdap-r0.4.0/log/lib
ENGINE_LIB=/data/bdap-r0.4.0/engine/lib
LOG_LIB=/data/bdap-r0.4.0/log/lib
export HADOOP_CONF_DIR=/data/hadoop-2.5.2/etc/hadoop/

$SPARK_HOME/bin/spark-submit \
  --class etl.log.StreamLogProcessor \
  --master yarn \
  --deploy-mode client \
  --supervise \
  --executor-memory 4G \
  --total-executor-cores 10 \
  --jars \
  $ENGINE_LIB/commons-cli-1.3.1.jar,$ENGINE_LIB/commons-codec-1.4.jar,$ENGINE_LIB/commons-collections-3.2.2.jar,$ENGINE_LIB/commons-compress-1.12.jar,$ENGINE_LIB/commons-configuration-1.7.jar,$ENGINE_LIB/commons-csv-1.4.jar,$ENGINE_LIB/commons-daemon-1.0.13.jar,$ENGINE_LIB/commons-dbcp-1.4.jar,$ENGINE_LIB/commons-el-1.0.jar,$ENGINE_LIB/commons-exec-1.2.jar,$ENGINE_LIB/commons-httpclient-3.1.jar,$ENGINE_LIB/commons-io-2.5.jar,$ENGINE_LIB/commons-lang-2.6.jar,$ENGINE_LIB/commons-lang3-3.3.2.jar,$ENGINE_LIB/commons-logging-1.1.3.jar,$ENGINE_LIB/commons-math3-3.1.1.jar,$ENGINE_LIB/commons-net-3.1.jar,$ENGINE_LIB/commons-pool-1.5.4.jar,$ENGINE_LIB/jackson-annotations-2.6.0.jar,$ENGINE_LIB/jackson-core-2.6.5.jar,$ENGINE_LIB/jackson-databind-2.6.5.jar,$ENGINE_LIB/jsch-0.1.53.jar,$ENGINE_LIB/kafka-clients-0.10.0.1.jar,$ENGINE_LIB/log4j-api-2.6.2.jar,$ENGINE_LIB/log4j-core-2.6.2.jar,$ENGINE_LIB/vertica-jdbc-7.0.1-0.jar,$ENGINE_LIB/spark-streaming-kafka-0-10_2.11-2.0.0.jar,$ENGINE_LIB/bdap.common-r0.4.0.jar,$ENGINE_LIB/bdap.engine-r0.4.0.jar \
  $LOG_LIB/bdap.log-r0.4.0.jar