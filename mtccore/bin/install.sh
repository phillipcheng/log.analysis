user=chengyi

hdfs dfs -rm -r /mtccore/etlcfg
hdfs dfs -mkdir -p /mtccore/etlcfg
for f in etlcfg/*
do
	hdfs dfs -copyFromLocal $f /mtccore/$f
done

hdfs dfs -mkdir -p /mtccore/xmldata
hdfs dfs -mkdir -p /mtccore/csvdata
hdfs dfs -mkdir -p /mtccore/schemahistory

hdfs dfs -rm -r /mtccore/schema
hdfs dfs -mkdir -p /mtccore/schema
hdfs dfs -copyFromLocal smsc.schema /mtccore/schema/smsc.schema
hdfs dfs -copyFromLocal sgsiwf.schema /mtccore/schema/sgsiwf.schema

hdfs dfs -rm -r /user/$user/mtccore/
hdfs dfs -mkdir -p /user/$user/mtccore/lib
for f in *.jar
do 
	hdfs dfs -copyFromLocal $f /user/$user/mtccore/lib/$f
done

hdfs dfs -copyFromLocal etlengine.properties /user/$user/mtccore/lib/etlengine.properties

for f in *.workflow.xml
do
	hdfs dfs -copyFromLocal $f /user/$user/mtccore/$f
done
