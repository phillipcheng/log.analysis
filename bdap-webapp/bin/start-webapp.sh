java -Dhadoop.home.dir=/data/hadoop-2.7.3 -Djava.library.path=/data/hadoop-2.7.3/lib/native/ -cp BOOT-INF/lib/*:.:BOOT-INF/classes/ dv.Application > bdap.webapp.log &
disown

