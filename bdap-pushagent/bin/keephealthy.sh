#!/bin/bash
basepath=$(cd `dirname $0`; pwd)
#echo $basepath >> ${basepath}/keephealthy.log
process=$(ps -ef | grep bdap.pushagent-r0.3-jdk1.7.jar | grep -v grep)
if [ -z "$process" ];
then
	echo "`date +%Y-%m-%d` `date +%H:%M:%S`  Starting push agent."  >> ${basepath}/keephealthy.log
	java -jar ${basepath}/bdap.pushagent-r0.3-jdk1.7.jar file://${basepath}/config.json >> ${basepath}/log.log 2>&1 &
	echo "`date +%Y-%m-%d` `date +%H:%M:%S`  Push agent is started." >> ${basepath}/keephealthy.log
else
	echo "`date +%Y-%m-%d` `date +%H:%M:%S`  Push agent already running." >> ${basepath}/keephealthy.log
	exit 1
fi
