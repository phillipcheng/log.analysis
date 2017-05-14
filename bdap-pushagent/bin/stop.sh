#!/bin/bash

process=$(ps -ef | grep bdap.pushagent-r0.5.0.jar | grep -v grep)
if [ -z "$process" ];
then
	echo "Push agent is not running."
	exit 1
else
	pid=$(echo $process| awk '{print $2}')
	echo "Starting to stop push agent ${pid}."
	kill -9 $pid
	echo "Stop push agent."
fi