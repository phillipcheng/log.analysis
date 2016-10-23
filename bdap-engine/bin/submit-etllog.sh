SPARK_HOME=/data/spark-2.0.0-bin-hadoop2.7
APPLIB=local:/data/preload/lib

$SPARK_HOME/bin/spark-submit \
  --class etl.log.StreamLogProcessor \
  --master spark://192.85.247.104:6066 \
  --deploy-mode cluster \
  --executor-memory 20G \
  --total-executor-cores 100 \
  /data/preload/lib/preload-0.1.0-all.jar
