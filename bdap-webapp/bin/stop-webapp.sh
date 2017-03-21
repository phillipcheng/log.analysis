#!/bin/bash

process=$(ps -ef | grep dv.Application | grep -v grep)
if [ -z "$process" ];
then
	echo "Webapp is not running."
	exit 1
else
	pid=$(echo $process| awk '{print $2}')
	echo "Starting to stop webapp ${pid}."
	kill -9 $pid
	echo "Stop webapp."
fi