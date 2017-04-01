#use hdfs user to execute
bdap_dir=/bdap-r0.5.0


#copy lib
hdfs dfs -rm -r ${bdap_dir}/engine/lib
hdfs dfs -mkdir -p ${bdap_dir}/engine/lib

for f in ../lib/*.jar
do
	hdfs dfs -copyFromLocal -f $f ${bdap_dir}/engine/lib/`basename $f`
done

#copy cfg files
hdfs dfs -rm -r ${bdap_dir}/cfg
hdfs dfs -mkdir -p ${bdap_dir}/cfg

for f in ../cfg/*
do
	hdfs dfs -copyFromLocal -f $f ${bdap_dir}/cfg/`basename $f`
done