#use hdfs user not oozie user to execute
oozie_user=dbadmin
java_home=/usr/lib/jvm/java-1.8.0/

#make lib
$java_home/bin/jar -uf ../bdap.engine-VVERSIONN.jar ../etlengine.properties

#copy lib
hdfs dfs -rm -r /user/$oozie_user/bdap-VVERSIONN/lib
hdfs dfs -mkdir -p /user/$oozie_user/bdap-VVERSIONN/lib

for f in ../lib/*.jar
do
	hdfs dfs -copyFromLocal -f $f /user/$oozie_user/bdap-VVERSIONN/lib/`basename $f`
done

hdfs dfs -rm -r /user/$oozie_user/bdap-VVERSIONN/jars
hdfs dfs -mkdir -p /user/$oozie_user/bdap-VVERSIONN/jars
hdfs dfs -copyFromLocal -f ../bdap.common-VVERSIONN.jar /user/$oozie_user/bdap-VVERSIONN/jars/
hdfs dfs -copyFromLocal -f ../bdap.engine-VVERSIONN.jar /user/$oozie_user/bdap-VVERSIONN/jars/

#copy schema file
hdfs dfs -mkdir -p /bdap-VVERSIONN/schema/
hdfs dfs -copyFromLocal -f ../schema/logschema.txt /bdap-VVERSIONN/schema/
