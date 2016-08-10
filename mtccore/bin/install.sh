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

hdfs dfs -rm -r /user/oozie/mtccore/
hdfs dfs -mkdir -p /user/oozie/mtccore/lib
for f in *.jar
do 
	hdfs dfs -copyFromLocal $f /user/oozie/mtccore/lib/$f
done

hdfs dfs -copyFromLocal etlengine.properties /user/oozie/mtccore/lib/etlengine.properties

for f in *.workflow.xml
do
	hdfs dfs -copyFromLocal $f /user/oozie/mtccore/$f
done
