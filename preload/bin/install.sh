#use hdfs user not oozie user to execute
hdfs dfs -rm -r /user/oozie/share/lib/preload/lib
hdfs dfs -mkdir -p /user/oozie/share/lib/preload/lib

for f in lib/*.jar
do 
	hdfs dfs -copyFromLocal $f /user/oozie/share/lib/preload/$f
done
