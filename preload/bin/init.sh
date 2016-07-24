KAFKA_ROOT=/data/kafka_2.11-0.10.0.0

$KAFKA_ROOT/bin/kafka-topics.sh --create --zookeeper 192.85.247.104:2181,192.85.247.105:2181,192.85.247.106:2181 --replication-factor 3 --partitions 3 --topic log-analysis-topic