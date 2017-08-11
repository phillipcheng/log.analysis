#!/bin/bash

PATH_DIR=$PWD
cd `dirname $0`/..
HOME_DIR=$PWD
cd $PATH_DIR


# java classpath
export CLASSPATH="$(echo $HOME_DIR/lib/*.jar | tr ' ' ':')"

java -cp $CLASSPATH -Dlog4j.configurationFile=$HOME_DIR/conf/log4j2.xml bdap.tools.jobHelper.JobHelper -config file://$HOME_DIR/conf/config.properties $*

#>> log.log 2>&1 