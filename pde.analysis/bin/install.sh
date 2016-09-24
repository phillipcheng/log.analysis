oozie_user=root
ssh_user=root
KAFKA_ROOT=/usr/hdp/current/kafka-broker
pde_home=/hadoop/pde/
hostarr=("10.194.205.138" "10.194.205.139")
zookeeper_url=10.194.205.137:2181,10.194.205.138:2181,10.194.205.139:2181

su - hdfs -c "hdfs dfs -rm -R -f /pde"
su - hdfs -c "hdfs dfs -mkdir -p /pde"
su - hdfs -c "hdfs dfs -chown -R $oozie_user /pde"
su - hdfs -c "hdfs dfs -chmod -R 777 /user"

hdfs dfs -rm -r /pde/etlcfg
hdfs dfs -mkdir -p /pde/etlcfg
for f in etlcfg/*
do
	hdfs dfs -copyFromLocal $f /pde/$f
done

hdfs dfs -mkdir -p /pde/rawinput
hdfs dfs -mkdir -p /pde/csv
hdfs dfs -mkdir -p /pde/transcsv
hdfs dfs -mkdir -p /pde/fix
hdfs dfs -mkdir -p /pde/fixcsv
hdfs dfs -mkdir -p /pde/mergecsv
hdfs dfs -mkdir -p /pde/ses
hdfs dfs -mkdir -p /pde/sescsv
su - hdfs -c "hdfs dfs -chmod -R 777 /pde"

hdfs dfs -rm -r /user/$oozie_user/pde/
hdfs dfs -mkdir -p /user/$oozie_user/pde/lib
for f in *.jar
do 
	hdfs dfs -copyFromLocal $f /user/$oozie_user/pde/lib/$f
done

hdfs dfs -copyFromLocal etlengine.properties /user/$oozie_user/pde/lib/etlengine.properties
hdfs dfs -copyFromLocal log4j2.xml /user/$oozie_user/pde/lib/log4j2.xml
hdfs dfs -copyFromLocal sitename_mapping.properties /user/$oozie_user/pde/lib/sitename_mapping.properties

for f in *workflow.xml *.coordinator.xml
do
	hdfs dfs -copyFromLocal $f /user/$oozie_user/pde/$f
done

mkdir -p ./tmp

for host in ${hostarr[@]}
do
	ssh $ssh_user@$host mkdir -p $pde_home/tmp
	ssh $ssh_user@$host chmod 777 $pde_home/tmp
	ssh $ssh_user@$host rm -rf $pde_home/tmp/*
	ssh $ssh_user@$host rm -rf $pde_home/bin/*
	rsync -a $pde_home/bin $ssh_user@$host:$pde_home
done

#$KAFKA_ROOT/bin/kafka-topics.sh --create --zookeeper $zookeeper_url --replication-factor 1 --partitions 3 --topic log-analysis-topic
