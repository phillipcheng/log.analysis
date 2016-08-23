#use hdfs user not oozie user to execute
user=chengyi

hdfs dfs -rm -r /user/$user/share/lib/preload/lib
hdfs dfs -mkdir -p /user/$user/share/lib/preload/lib

for f in lib/*.jar
do 
	hdfs dfs -copyFromLocal $f /user/$user/share/lib/preload/$f
done
