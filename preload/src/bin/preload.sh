#/bin/bash
set JAVA_HOME=
set LA_HOME=

CL=

for file in $LA_HOME/../lib/*
do
	CL=$CL:$file
done
echo $CL

$JAVA_HOME/bin/java -Xmx2048m -cp "$LA_EXT:$LA_HOME/preload-0.0.1.jar:$CL" log.analysis.preload.Preload $@