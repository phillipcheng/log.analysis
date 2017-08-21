You can use the root user to create a new crontab task

1.use "crontab  -e" edit crontab job to add:
	*/5 * * * * /bin/sh <pushagent_home>/bin/keephealthy.sh
example: */5 * * * * /bin/sh /root/pushagent/bin/keephealthy.sh

2.use "crontab -l" to check add job