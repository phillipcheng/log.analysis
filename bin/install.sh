#use hdfs user not oozie user to execute
oozie_user=dbadmin
java_home=/usr/lib/jvm/java-1.8.0/

#make lib
$java_home/bin/jar -uf ../bdap.engine-r0.3-jdk1.7.jar ../etlengine.properties

#copy lib
hdfs dfs -rm -r /user/$oozie_user/bdap-r0.3-jdk1.7/lib
hdfs dfs -mkdir -p /user/$oozie_user/bdap-r0.3-jdk1.7/lib

for f in ../lib/*.jar
do
	hdfs dfs -copyFromLocal -f $f /user/$oozie_user/bdap-r0.3-jdk1.7/lib/`basename $f`
done

hdfs dfs -rm -r /user/$oozie_user/bdap-r0.3-jdk1.7/jars
hdfs dfs -mkdir -p /user/$oozie_user/bdap-r0.3-jdk1.7/jars
hdfs dfs -copyFromLocal -f ../bdap.common-r0.3-jdk1.7.jar /user/$oozie_user/bdap-r0.3-jdk1.7/jars/
hdfs dfs -copyFromLocal -f ../bdap.engine-r0.3-jdk1.7.jar /user/$oozie_user/bdap-r0.3-jdk1.7/jars/

#copy schema file
hdfs dfs -mkdir -p /bdap-r0.3-jdk1.7/schema/
hdfs dfs -copyFromLocal -f ../schema/logschema.txt /bdap-r0.3-jdk1.7/schema/
