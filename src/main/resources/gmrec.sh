#!/bin/sh
rm /home/hadoop/gmrec.log
hadoop jar /home/hadoop/gmrec-3.0.jar com.elex.gmrec.Scheduler 0 4

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
bakdir=`date "+%Y%m%d"`
cp -R /home/hadoop/rec '/home/hadoop/backup_rec/'$bakdir
mailx -s "gmrecv3.0" wuzhongju@126.com </home/hadoop/gmrec.log
