#use hdfs user not oozie user to execute
oozie_user=dbadmin
java_home=/usr/lib/jvm/java-1.8.0/

#make lib
$java_home/bin/jar -uf bdap.engine-VERSION.jar etlengine.properties

#copy lib
hdfs dfs -rm -r /user/$oozie_user/share/lib/preload/lib
hdfs dfs -mkdir -p /user/$oozie_user/share/lib/preload/lib

for f in lib/*.jar
do 
	hdfs dfs -copyFromLocal -f $f /user/$oozie_user/share/lib/preload/$f
done

hdfs dfs -rm -r /user/$oozie_user/preload/jars
hdfs dfs -mkdir -p /user/$oozie_user/preload/jars
hdfs dfs -copyFromLocal -f bdap.common-VERSION.jar /user/$oozie_user/preload/jars/
hdfs dfs -copyFromLocal -f bdap.engine-VERSION.jar /user/$oozie_user/preload/jars/

#copy schema file
hdfs dfs -mkdir -p /preload/schema/
hdfs dfs -copyFromLocal -f schema/logschema.txt /preload/schema/
