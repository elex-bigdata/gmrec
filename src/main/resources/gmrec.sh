#!/bin/sh
hadoop jar /home/hadoop/gmrec-2.0.jar com.elex.gmrec.Scheduler 0 4
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

userrec=$1'/userrec'
hadoop fs -getmerge $2'/recmerge/user' "$userrec"
userrecmd5=`/usr/java/jdk/bin/java -jar /home/hadoop/MD5Digest.jar $userrec`
rm -rf $1'/user'
mkdir $1'/user'
mv $userrec $1'/user/'$userrecmd5
echo "userrec load success !!!"

itemrec=$1'/itemrec'
hadoop fs -getmerge $2'/recmerge/item' "$itemrec"
itemrecmd5=`/usr/java/jdk/bin/java -jar /home/hadoop/MD5Digest.jar $itemrec`
rm -rf $1'/item'
mkdir $1'/item'
mv $itemrec $1'/item/'$itemrecmd5
echo "itemrec load success !!!"


scp -r /home/hadoop/rec elex@162.243.114.236:~
echo "copy result to vps sucess !!!"
