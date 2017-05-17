#use hdfs user to execute
bdap_dir=/bdap-VVERSIONN


#copy lib
hdfs dfs -rm -r ${bdap_dir}/engine/lib
hdfs dfs -mkdir -p ${bdap_dir}/engine/lib

for f in ../lib/*.jar
do
	hdfs dfs -copyFromLocal -f $f ${bdap_dir}/engine/lib/`basename $f`
done

#copy cfg files
hdfs dfs -rm -r ${bdap_dir}/engine/cfg
hdfs dfs -mkdir -p ${bdap_dir}/engine/cfg/lib

for f in ../cfg/*
do
    hdfs dfs -copyFromLocal -f $f ${bdap_dir}/engine/cfg/`basename $f`
done
hdfs dfs -mv ${bdap_dir}/engine/cfg/submitspark.sh ${bdap_dir}/engine/cfg/lib/submitspark.sh
