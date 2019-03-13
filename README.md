# Migrate HDFS Store to Azure Data Lake Storage Gen2

The key challenge for customers with existing on-premises Hadoop clusters that wish to migrate to Azure (or exist in a hybrid environment) is the movement of the existing dataset. The dataset may be very large, which likely rules out online transfer. Transfer volume can be solved by using Azure Databox as a physical appliance to 'ship' the data to Azure.

This set of scripts provides specific support for moving big data analytics datasets from an on-premises HDFS cluster to ADLS Gen2 using a variety of Hadoop and custom tooling.

## Prerequisites

The mechanism to copy data from an on-premise HDFS cluster to ADLS Gen2 relies on the following:

1. A Hadoop cluster containing the source data to be migrated.
2. A head or edge node on the above cluster that you can SSH onto with python (>= 2.7 or 3) installed with `pip`.
2. A Databox.
3. A Hadoop cluster running in Azure (eg. HDInsight, etc.).

## Process - Overview

1. Use the Hadoop tool `distcp` to copy data from the source HDFS cluster to the Databox.
2. Ship the Databox to Azure and have the data loaded into a non-HNS enabled Storage Account
3. Use the Hadoop tool `distcp` to copy data from the non-HNS enabled Storage Account to the HNS-enabled ADLS Gen2 account
4. Copy and translate permissions from the HDFS cluster to the ADLS Gen2 account using the supplied scripts

## Step 1 - Distcp data from HDFS to Databox

1. Setup the Databox onto the on-premise network following instructions here: <<TODO: Add link>>
2. Use cluster management tools to add the Databox DNS name to every node's `/etc/hosts` file
3. When using `distcp` to copy files from the on-premise Hadoop cluster to the Databox, some directories will need to be excluded (they generally contain state information to keep the cluster running and so are not important to copy). The `distcp` tool supports a mechanism to exclude files & directories by specifying a series of regular expressions (1 per line) that exclude matching paths. On the on-premise Hadoop cluster where you will be initiating the `distcp` job, create a file with the list of directories to exclude, similar to the following:
```
exclusions.lst

.*ranger/audit.*
.*/hbase/data/WALs.*
```
4. On the on-premise Hadoop cluster, run the following `distcp` job to copy data and metadata from HDFS to Databox:
```bash
sudo -u hdfs hadoop distcp -Dfs.azure.account.key.{databox_dns}={databox_key} -filter ./exclusions.lst /[source directory] wasb://{container}@{databox_dns}/[path]
```
## Step 2 - Ship the Databox to Microsoft

Now that the Databox is fully loaded with a copy of the HDFS data, prepare and ship the Databox back to Microsoft <<TODO: Add link>>. The data will be loaded into the account (with HNS disabled) you specified when ordering the Databox.

## Step 3 - Copy data from HNS disabled account to ADLS Gen2 account

1. On the cloud-based Hadoop cluster (eg. HDInsight), run the following `distcp` command to copy the data and the metadata for the account that Databox loaded the data into (with HNS disabled) into your ADLS Gen2 account. It is assumed that the destination ADLS Gen2 account has already been configured on the cluster:
```bash
hadoop distcp -Dfs.azure.account.key.{source_account}.dfs.windows.net={source_account_key} abfs://{source_container}@{source_account}.dfs.windows.net/[path] abfs://{dest_container}@{dest_account}.dfs.windows.net/[path]
```

## Step 4 - Copy and map identities and permissions from HDFS to ADLS Gen2

1. On the on-premise Hadoop cluster edge or head node, execute the following command to clone this Github repo. This will download the necessary scripts to the local computer:
```bash
git clone https://github.com/jamesbak/databox-adls-loader.git
cd databox-adls-loader
```
2. Ensure that the `jq` package is installed. Eg. For Ubuntu:
```bash
sudo apt-get install jq
``` 
3. Install the `requests` python package:
```bash
pip install requests
```
4. Set execute permissions on the required scripts
```bash
chmod +x *.py *.sh
```
5. Create a service principal & grant 'Storage Blobs Data Owner' role membership. Record the client id & secret, so that these values can be used to authenticate to the ADLS Gen2 account in the next step.
6. On the on-premise Hadoop cluster, execute the following Bash command to generate a list of copied 
files with their permissions (depending on the number of files in HDFS, this command may take a long time to run):
```bash
sudo -u hdfs ./copy-acls.sh -s /{hdfs_path} > ./filelist.json
```
7. Generate the list of unique identities that need to be mapped to AAD-based identities:
```bash
./copy-acls.py -s ./filelist.json -i id_map.json -g
```
8. Using a text editor open the generated `id_map.json` file. For each JSON object in the file, update the `target` attribute (either an AAD User Principal Name (UPN) or objectId (OID)) with the mapped identity. Once complete save the file for use in the next step.
9. Run the following script to apply permissions to the copied data in the ADLS Gen2 account:
```bash
./copy-acls.py -s ./filelist.json -i ./id_map.json  -A adlsgen2hnswestus2 -C databox1 --dest-spn-id {spn_client_id}  --dest-spn-secret {spn_secret}
```

# Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
