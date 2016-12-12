#use hdfs user not oozie user to execute
bdap_dir=/bdap-r0.3-jdk1.7
java_home=/usr/lib/jvm/java-1.8.0/

#make lib
$java_home/bin/jar -uf ../lib/bdap.log-r0.3-jdk1.7.jar -C ../cfg etlengine.properties -C ../cfg log4j.properties -C ../cfg log4j2.xml

hdfs dfs -rm -r ${bdap_dir}/log/

#copy lib
hdfs dfs -mkdir -p ${bdap_dir}/log/lib
hdfs dfs -copyFromLocal -f ../lib/bdap.log-r0.3-jdk1.7.jar ${bdap_dir}/log/lib/

#copy schema file
hdfs dfs -mkdir -p ${bdap_dir}/log/schema/
hdfs dfs -copyFromLocal -f ../schema/logschema.txt ${bdap_dir}/log/schema/
