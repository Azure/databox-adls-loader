#!/bin/bash

tarballs="$(hadoop fs -ls -R /hdp | tr -s ' ' | cut -d ' ' -f8 | grep .tar.gz$)"
for tar in $tarballs
do
        hadoop fs -copyToLocal $tar .
        tarname=$(basename $tar)
        tarfile="$(readlink -f $tarname)"
        tarprefix="${tarname%%.*}"
        echo "Found: $tarname"
        mkdir $tarprefix
        tar -xzf $tarfile -C $tarprefix
        find $tarprefix -name azure*storage*jar -or -name hadoop-azure-2*.jar | egrep '.*' >/dev/null
        if [ $? -eq 0 ];
        then
                echo "Processing: $tarname"
                find $tarprefix -name azure*storage*jar -or -name hadoop-azure-2*.jar | xargs rm
                cd $tarprefix
                tar -zcf $tarfile *
                cd ..
                hadoop fs -mv $tar $tar.save
                hadoop fs -copyFromLocal -p -f $tarfile $tar
        fi
        rm -R $tarprefix
        rm $tarfile
done