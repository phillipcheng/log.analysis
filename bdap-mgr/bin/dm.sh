java_home=/usr/lib/jvm/java-1.8.0/
bdap_home=/data/bdap-bdap-r0.6.0

$java_home/bin/java -cp .:"$bdap_home/mgr/lib/*" etl.flow.deploy.FlowDeployer "$@"
