#!/bin/sh
lf='/home/hadoop/cf'
hadoop fs -get /gmrec/parse/cf "$lf"
a=`java -jar /home/hadoop/MD5Digest.jar $lf`
rm -rf /home/hadoop/cfrec
mkdir /home/hadoop/cfrec
mv $lf /home/hadoop/cfrec/$a 
echo "$a"
