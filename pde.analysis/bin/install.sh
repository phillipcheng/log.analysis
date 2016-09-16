user=dbadmin

hdfs dfs -rm -r /pde/etlcfg
hdfs dfs -mkdir -p /pde/etlcfg
for f in etlcfg/*
do
	hdfs dfs -copyFromLocal $f /pde/$f
done

hdfs dfs -mkdir -p /pde/rawinpug
hdfs dfs -mkdir -p /pde/csv
hdfs dfs -mkdir -p /pde/fix
hdfs dfs -mkdir -p /pde/fixcsv
hdfs dfs -mkdir -p /pde/mergecsv
hdfs dfs -mkdir -p /pde/finalcsv
hdfs dfs -mkdir -p /pde/ses
hdfs dfs -mkdir -p /pde/sescsv

hdfs dfs -rm -r /user/$user/pde/
hdfs dfs -mkdir -p /user/$user/pde/lib
for f in *.jar
do 
	hdfs dfs -copyFromLocal $f /user/$user/pde/lib/$f
done

hdfs dfs -copyFromLocal etlengine.properties /user/$user/pde/lib/etlengine.properties

for f in *workflow.xml *.properties
do
	hdfs dfs -copyFromLocal $f /user/$user/pde/$f
done

mkdir tmp
