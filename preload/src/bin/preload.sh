#/bin/bash

JAVA_HOME=/usr/lib/jvm/jre-1.7.0-openjdk.x86_64
LA_HOME=/data/log.analysis

CL=

for file in $LA_HOME/lib/*
do
	CL=$CL:$file
done
echo $CL

$JAVA_HOME/bin/java -Xmx2048m -cp "$LA_EXT:$LA_HOME/preload-0.0.1.jar:$CL" log.analysis.preload.Preload $@