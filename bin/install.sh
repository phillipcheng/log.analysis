#use hdfs user not oozie user to execute
oozie_user=dbadmin
java_home=/usr/lib/jvm/java-1.8.0/

#make lib
$java_home/bin/jar -uf ../bdap.engine-r0.4.0.jar ../etlengine.properties

#copy lib
hdfs dfs -rm -r /user/$oozie_user/bdap-r0.4.0/lib
hdfs dfs -mkdir -p /user/$oozie_user/bdap-r0.4.0/lib

for f in ../lib/*.jar
do
	hdfs dfs -copyFromLocal -f $f /user/$oozie_user/bdap-r0.4.0/lib/`basename $f`
done

hdfs dfs -rm -r /user/$oozie_user/bdap-r0.4.0/jars
hdfs dfs -mkdir -p /user/$oozie_user/bdap-r0.4.0/jars
hdfs dfs -copyFromLocal -f ../bdap.common-r0.4.0.jar /user/$oozie_user/bdap-r0.4.0/jars/
hdfs dfs -copyFromLocal -f ../bdap.engine-r0.4.0.jar /user/$oozie_user/bdap-r0.4.0/jars/

#copy schema file
hdfs dfs -mkdir -p /bdap-r0.4.0/schema/
hdfs dfs -copyFromLocal -f ../schema/logschema.txt /bdap-r0.4.0/schema/
