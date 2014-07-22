#!/bin/sh
hadoop jar /home/hadoop/gmrec-1.0.jar com.elex.gmrec.Scheduler 0 2
cf=$1'/cf'
hadoop fs -get $2'/parse/cf' "$cf"
cfmd5=`/usr/java/jdk/bin/java -jar /home/hadoop/MD5Digest.jar $cf`
rm -rf $1'/cfrec'
mkdir $1'/cfrec'
mv $cf $1'/cfrec/'$cfmd5 
echo "cfrec load success !!!"

cfsim=$1'/cfsim'
hadoop fs -getmerge $2'/output/cfsim' "$cfsim"
cfsimmd5=`/usr/java/jdk/bin/java -jar /home/hadoop/MD5Digest.jar $cfsim`
rm -rf $1'/simrec'
mkdir $1'/simrec'
mv $cfsim $1'/simrec/'$cfsimmd5 
echo "cfsim load success !!!"

ar=$1'/ar'
hadoop fs -get $2'/output/arule/rule' "$ar"
armd5=`/usr/java/jdk/bin/java -jar /home/hadoop/MD5Digest.jar $ar`
rm -rf $1'/arrec'
mkdir $1'/arrec'
mv $ar $1'/arrec/'$armd5 
echo "arrec load success !!!"
scp -r /home/hadoop/rec elex@162.243.114.236:~
echo "copy result to vps sucess !!!"
