#!/bin/sh

SOURCE_FILE_LIST=$1
DEST_DNS_NAME=$2
DEST_ACCOUNT_KEY=$3
CONTAINER=$4
JOB_QUEUE=${5:-default}
NUM_MAPPERS=${6:-4}
HADOOP_OPTS=$7

if [ ! -f $SOURCE_FILE_LIST ] || [ -z $DEST_DNS_NAME ] || [ -z $CONTAINER ];
then
    echo "Usage: $0 {source filelist file} {databox DNS name} {databox account key} {databox container} [{YARN queue}] [{mappers}] [{Hadoop opts}]"
    exit 1
fi

DEST_ROOT_PATH=wasb://$CONTAINER@$DEST_DNS_NAME
while read path; 
do
    # Test if the destination already exists
    DEST_PATH=$DEST_ROOT_PATH$path
    hadoop fs $HADOOP_OPTS -D fs.azure.account.key.$DEST_DNS_NAME=$DEST_ACCOUNT_KEY -test -e $DEST_PATH
    if [ $? -eq 0 ]; then
        DEST_PATH=${DEST_PATH%/*}
    fi
    DEST_PATH=$DEST_PATH/
    hadoop distcp $HADOOP_OPTS -D fs.azure.account.key.$DEST_DNS_NAME=$DEST_ACCOUNT_KEY -Dmapred.job.queue.name=$JOB_QUEUE -m $NUM_MAPPERS -async $path $DEST_PATH
done < $SOURCE_FILE_LIST