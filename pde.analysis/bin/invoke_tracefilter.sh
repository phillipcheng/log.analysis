#!/bin/bash
set -x

JAVA_HOME=/usr/lib/jvm/jre-1.7.0-openjdk.x86_64
HADOOP_HOME=/data/hadoop-2.5.2
PDE_ANALYSIS=/data/pde

local_work_folder=$1
hdfs_work_folder=$2
rawfile_file=$3
hdfs_csv_folder=$4
hdfs_fix_folder=$5
hdfs_ses_folder=$6
wfid=$7

base_file=${rawfile_file:${#rawfile_file}-17:13}
echo $base_file

#copy the input file from rawfile_folder on hdfs to local work.folder
$HADOOP_HOME/bin/hdfs dfs -copyToLocal $rawfile_file $local_work_folder

#run tracefilter2 to decode bin files
$PDE_ANALYSIS/bin/TraceFilter2 /f $local_work_folder//$base_file.bin /ab /n /y /nm /ses /fix /h /hst

#copy fix and csv files to hdfs
$HADOOP_HOME/bin/hdfs dfs -mkdir -p $hdfs_work_folder/$hdfs_csv_folder/$wfid/
$HADOOP_HOME/bin/hdfs dfs -mkdir -p $hdfs_work_folder/$hdfs_fix_folder/$wfid/
$HADOOP_HOME/bin/hdfs dfs -mkdir -p $hdfs_work_folder/$hdfs_ses_folder/$wfid/
$HADOOP_HOME/bin/hdfs dfs -copyFromLocal $local_work_folder/$base_file.csv $hdfs_work_folder/$hdfs_csv_folder/$wfid/$base_file.csv
$HADOOP_HOME/bin/hdfs dfs -copyFromLocal $local_work_folder/$base_file.fix $hdfs_work_folder/$hdfs_fix_folder/$wfid/$base_file.fix
$HADOOP_HOME/bin/hdfs dfs -copyFromLocal $local_work_folder/$base_file.ses $hdfs_work_folder/$hdfs_ses_folder/$wfid/$base_file.ses

#remove the temp files
rm -f $local_work_folder/$base_file.*