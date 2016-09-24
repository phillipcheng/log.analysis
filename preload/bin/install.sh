#use hdfs user not oozie user to execute
user=chengyi

hdfs dfs -rm -r /user/$user/preload/lib
hdfs dfs -mkdir -p /user/$user/preload/lib

for f in lib/*.jar
do 
	hdfs dfs -copyFromLocal $f /user/$user/preload/$f
done
