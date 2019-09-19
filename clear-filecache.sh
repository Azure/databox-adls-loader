#!/bin/bash

AMBARI_SERVER=$1    
AMBARI_USER=$2
AMBARI_PASSWORD=$3

echo "Updating Hadoop MapReduce framework archive that is used via the distributed cache" 
if [ -z "$AMBARI_SERVER" ] || [ -z "$AMBARI_USER" ] || [ -z "$AMBARI_PASSWORD" ];
then
    echo "Usage $0 {Ambari host} {Ambari User} {Ambari Password}"
    exit 1
fi
# Read the location of the mapreduce framework archive - we may need to do some variable substitution
cluster=$(curl --user $AMBARI_USER:$AMBARI_PASSWORD "$AMBARI_SERVER/api/v1/clusters" | jq -r '.items[0].Clusters.cluster_name')
config_version=$(curl --user $AMBARI_USER:$AMBARI_PASSWORD "$AMBARI_SERVER/api/v1/clusters/$cluster?fields=Clusters/desired_configs" | jq -r '.Clusters.desired_configs."mapred-site".tag')
mapred_fx_path=$(curl --user $AMBARI_USER:$AMBARI_PASSWORD "$AMBARI_SERVER/api/v1/clusters/$cluster/configurations?type=mapred-site&tag=$config_version" | jq -r '.items[0].properties."mapreduce.application.framework.path"')
# Variable substitution
hdp_version=$(hdp-select versions)
mapred_fx_path=$(echo $mapred_fx_path | sed -e 's/${hdp.version}/'$hdp_version'/g')
mapred_fx_path=$(echo $mapred_fx_path | cut -d '#' -f1)

# Now that we have our location, download the archive from HDFS, make adjustments, re-tar the archive & copy it back to HDFS
echo "Searching for archives: $mapred_fx_path"
tarballs="$(hadoop fs -ls $mapred_fx_path | tr -s ' ' | cut -d ' ' -f8)"
for tar in $tarballs
do
        hadoop fs -copyToLocal $tar .
        tarname=$(basename $tar)
        tarfile="$(readlink -f $tarname)"
        tarprefix="${tarname%%.*}"
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