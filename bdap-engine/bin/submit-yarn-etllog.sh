JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk.x86_64/
SPARK_HOME=/data/spark-2.0.0-bin-hadoop2.7
HDFS_APPLIB=hdfs://192.85.247.104:19000/user/dbadmin/share/lib/preload/lib
export HADOOP_CONF_DIR=/data/hadoop-2.5.2/etc/hadoop/

$SPARK_HOME/bin/spark-submit \
  --class etl.log.StreamLogProcessor \
  --master yarn \
  --deploy-mode cluster \
  --supervise \
  --executor-memory 4G \
  --total-executor-cores 10 \
  --jars \
  $HDFS_APPLIB/commons-cli-1.3.1.jar,$HDFS_APPLIB/commons-codec-1.4.jar,$HDFS_APPLIB/commons-collections-3.2.1.jar,$HDFS_APPLIB/commons-compress-1.4.1.jar,$HDFS_APPLIB/commons-configuration-1.10.jar,$HDFS_APPLIB/commons-csv-1.4.jar,$HDFS_APPLIB/commons-daemon-1.0.13.jar,$HDFS_APPLIB/commons-dbcp-1.4.jar,$HDFS_APPLIB/commons-el-1.0.jar,$HDFS_APPLIB/commons-exec-1.2.jar,$HDFS_APPLIB/commons-httpclient-3.1.jar,$HDFS_APPLIB/commons-io-2.5.jar,$HDFS_APPLIB/commons-lang-2.6.jar,$HDFS_APPLIB/commons-lang3-3.3.2.jar,$HDFS_APPLIB/commons-logging-1.1.3.jar,$HDFS_APPLIB/commons-math3-3.1.1.jar,$HDFS_APPLIB/commons-net-3.1.jar,$HDFS_APPLIB/commons-pool-1.5.4.jar,$HDFS_APPLIB/jackson-annotations-2.6.0.jar,$HDFS_APPLIB/jackson-core-2.6.5.jar,$HDFS_APPLIB/jackson-databind-2.6.5.jar,$HDFS_APPLIB/jsch-0.1.53.jar,$HDFS_APPLIB/kafka-clients-0.10.0.1.jar,$HDFS_APPLIB/log4j-api-2.6.2.jar,$HDFS_APPLIB/log4j-core-2.6.2.jar,$HDFS_APPLIB/vertica-jdbc-7.0.1-0.jar,$HDFS_APPLIB/spark-streaming-kafka-0-10_2.11-2.0.0.jar \
  $HDFS_APPLIB/preload-0.1.0.jar