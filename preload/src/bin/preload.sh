#/bin/bash
CL=

for file in ./lib/*
do
	CL=$CL:$file
done
echo $CL
java -Xmx2048m -cp "target/classes:$CL" org.cld.book.Main