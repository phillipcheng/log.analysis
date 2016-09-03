srcfolder=$1
destfolder=$2
key=$3

echo $key

if [ -d $srcfolder ]
then 
	rm -rf $srcfolder
fi
mkdir -p $srcfolder

if [ -d $destfolder ]
then
	rm -rf $destfolder
fi
mkdir -p $destfolder

echo The test content goes here >$srcfolder/testShell.txt

cp $srcfolder/testShell.txt $destfolder/testShell.txt