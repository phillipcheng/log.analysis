HADOOP_ROOT=/Applications/hadoop-2.7.2/
HIVE_ROOT=/Applications/apache-hive-2.1.0-bin/
ZOOKEEPER_ROOT=/Applications/zookeeper-3.4.8/
KAFKA_ROOT=/Applications/kafka_2.11-0.10.0.1/
OOZIE_ROOT=/Applications/oozie-4.2.0/

SPARK_ROOT=/Applications/spark-2.0.0-bin-hadoop2.7/
$SPARK_ROOT/sbin/stop-all.sh

HiveMetaStore
HiveServer2
$OOZIE_ROOT/bin/oozied.sh stop (1)
$KAFKA_ROOT/bin/kafka-server-stop.sh $KAFKA_ROOT/config/server.properties (1)
$ZOOKEEPER_ROOT/bin/zkServer.sh stop (1)
$HADOOP_ROOT/sbin/mr-jobhistory-daemon.sh --config $HADOOP_ROOT/etc/hadoop stop historyserver (1)
$HADOOP_ROOT/sbin/stop-all.sh (2 dfs + 1 secondary, 2 yarn)

zookeeper start on all the servers

