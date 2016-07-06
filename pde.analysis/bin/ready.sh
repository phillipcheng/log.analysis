#/bin/bash
hostarr=("192.85.247.104" "192.85.247.105" "192.85.247.106")
for host in ${hostarr[@]}
do
	ssh dbadmin@$host mkdir /data/pde/tmp
	ssh dbadmin@$host rm -rf /data/pde/tmp/*
	rsync -a /data/pde/bin dbadmin@$host:/data/pde/
done