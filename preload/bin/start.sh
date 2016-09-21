HADOOP_ROOT=/Applications/hadoop-2.7.2/
HIVE_ROOT=/Applications/apache-hive-2.1.0-bin/
ZOOKEEPER_ROOT=/Applications/zookeeper-3.4.8/
KAFKA_ROOT=/Applications/kafka_2.11-0.10.0.1/
OOZIE_ROOT=/Applications/oozie-4.2.0/
SPARK_ROOT=/Applications/spark-2.0.0-bin-hadoop2.7/


$HADOOP_ROOT/sbin/start-all.sh
$HADOOP_ROOT/sbin/mr-jobhistory-daemon.sh --config $HADOOP_ROOT/etc/hadoop start historyserver
$ZOOKEEPER_ROOT/bin/zkServer.sh start
#nohup $HIVE_ROOT/bin/hiveserver2 &
#sleep zookeeper timeout (twice the ticktime)
sleep 4s
nohup $KAFKA_ROOT/bin/kafka-server-start.sh $KAFKA_ROOT/config/server.properties &
$OOZIE_ROOT/bin/oozied.sh start
$SPARK_ROOT/sbin/start-all.sh