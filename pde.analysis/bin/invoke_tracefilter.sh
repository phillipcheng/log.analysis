#!/bin/bash
set -x

HDFS_BIN=/usr/hdp/current/hadoop-hdfs-client/
pde_home=/hadoop/pde


local_work_folder=$1
hdfs_work_folder=$2
src_file=$3
hdfs_csv_folder=$4
hdfs_fix_folder=$5
hdfs_ses_folder=$6
wfid=$7
sftpHost=$8
sftpUser=$9

rawfile_file=`basename $src_file`
base_file=${rawfile_file:${#rawfile_file}-17:13}
echo $base_file

#sftp the input file from sftpHost to local work.folder
scp $sftpUser@$sftpHost:$src_file $local_work_folder
#run tracefilter2 to decode bin files
$pde_home/bin/TraceFilter2 /f $local_work_folder//$base_file.bin /ab /n /y /nm /ses /fix /h /hst

#copy fix and csv files to hdfs
$HDFS_BIN/bin/hdfs dfs -copyFromLocal $local_work_folder/$base_file.csv $hdfs_work_folder/$hdfs_csv_folder/$wfid/$base_file.csv
$HDFS_BIN/bin/hdfs dfs -copyFromLocal $local_work_folder/$base_file.fix $hdfs_work_folder/$hdfs_fix_folder/$wfid/$base_file.fix
$HDFS_BIN/bin/hdfs dfs -copyFromLocal $local_work_folder/$base_file.ses $hdfs_work_folder/$hdfs_ses_folder/$wfid/$base_file.ses

#remove the temp files
rm -f $local_work_folder/$base_file.*
ssh $sftpUser@$sftpHost rm -f $src_file