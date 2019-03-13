#!/bin/sh

SOURCE_PATH=$1
DEST_PATH=$2

if [ -z $SOURCE_PATH ] || [ -z $DEST_PATH ];
then
    echo "Usage: $0 {source path} {destination path}"
    exit 1
fi

echo "Generating source file list"
hadoop fs -ls -C $SOURCE_PATH | hadoop fs -put -f - ~/distcp.list
echo "Copying directories, files & permissions"
hadoop distcp -Dfs.azure.localuserasfileowner.replace.principals=$(whoami),supergroup -p -f ~/distcp.list $DEST_PATH
echo "Cleaning up"
hadoop fs -rm -skipTrash ~/distcp.list
