# Migrate HDFS Store to Azure Data Lake Storage Gen2

The key challenge for customers with existing on-premises Hadoop clusters that wish to migrate to Azure (or exist in a hybrid environment) is the movement of the existing dataset. The dataset may be very large, which likely rules out online transfer. Transfer volume can be solved by using Azure Data Box as a physical appliance to 'ship' the data to Azure.

This set of scripts provides specific support for moving big data analytics datasets from an on-premises HDFS cluster to ADLS Gen2 using a variety of Hadoop and custom tooling.

## Prerequisites

The mechanism to copy data from an on-premise HDFS cluster to ADLS Gen2 relies on the following:

1. A Hadoop cluster containing the source data to be migrated.
2. A Hadoop cluster running in Azure (eg. HDInsight, etc.).
3. An [Azure Data Box device](https://azure.microsoft.com/services/storage/databox/). 

    - [Order your Data Box](https://docs.microsoft.com/azure/databox/data-box-deploy-ordered). While ordering your Box, remember to choose a storage account that **doesn't** have hierarchical namespaces enabled on it. This is because Data Box does not yet support direct ingestion into Azure Data Lake Storage Gen2. You will need to copy into a storage account and then do a second copy into the ADLS Gen2 account. Instructions for this are given in the steps below.
    - [Cable and connect your Data Box](https://docs.microsoft.com/azure/databox/data-box-deploy-set-up) to an on-premises network.
4. A head or edge node on the above cluster that you can SSH onto with `python` (>= 2.7 or 3) installed with `pip`.

## Process - Overview

1. Clone this repo on the on-premise Hadoop cluster
2. Use the Hadoop tool `distcp` to copy data from the source HDFS cluster to the Data Box
3. Ship the Data Box to Azure and have the data loaded into a non-HNS enabled Storage Account
4. Use the Hadoop tool `distcp` to copy data from the non-HNS enabled Storage Account to the HNS-enabled ADLS Gen2 account
5. Translate and copy permissions from the HDFS cluster to the ADLS Gen2 account using the supplied scripts

## Step 1 - Clone Github repository to download required scripts

1. On the on-premise Hadoop cluster edge or head node, execute the following command to clone this Github repo. This will download the necessary scripts to the local computer:
    ```bash
    git clone https://github.com/Azure/databox-adls-loader.git
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
5. (Optional) If the WASB driver is not in the standard `CLASSPATH` set a shell variable `azjars` to point to the `hadoop-azure` and the `*azure-storage*` jar files. These files are under the Hadoop installation directory (You can check if these files exist by using this command `ls -l $<hadoop_install_dir>/share/hadoop/tools/lib/ | grep azure` where `<hadoop_install_dir>` is the directory where you have installed Hadoop). Use the full paths. Eg:

    ```
    azjars=$hadoop_install_dir/share/hadoop/tools/lib/hadoop-azure-2.6.0-cdh5.14.0.jar
    azjars=$azjars,$hadoop_install_dir/share/hadoop/tools/lib/microsoft-windowsazure-storage-sdk-0.6.0.jar
    ```

6. Create a service principal & grant 'Storage Blobs Data Owner' role membership. Record the client id & secret, so that these values can be used to authenticate to the ADLS Gen2 account in the steps below.


## Step 2 - Distcp data from HDFS to Data Box

1. Setup the Data Box onto the on-premise network following instructions here: [Cable and connect your Data Box](https://docs.microsoft.com/azure/databox/data-box-deploy-set-up)
2. Use cluster management tools to add the Data Box DNS name to every node's `/etc/hosts` file
3. (Optional) If the size of data you wish to migrate exceeds the size of a single Data Box you will need to split the copies over multiple Data Box instances. To generate a list of files that should be copied, run the following script from the previously cloned Github repo (note the elevated permissions):

    ```bash
    sudo -u hdfs ./generate-file-list.py [-h] [-s DATABOX_SIZE] [-b FILELIST_BASENAME]
                        [-f LOG_CONFIG] [-l LOG_FILE]
                        [-v {DEBUG,INFO,WARNING,ERROR}]
                        path

    where:
    positional arguments:
    path                  The base HDFS path to process.

    optional arguments:
    -h, --help            show this help message and exit
    -s DATABOX_SIZE, --databox-size DATABOX_SIZE
                            The size of each Data Box in bytes.
    -b FILELIST_BASENAME, --filelist-basename FILELIST_BASENAME
                            The base name for the output filelists. Lists will be
                            named basename1, basename2, ... .
    -f LOG_CONFIG, --log-config LOG_CONFIG
                            The name of a configuration file for logging.
    -l LOG_FILE, --log-file LOG_FILE
                            Name of file to have log output written to (default is
                            stdout/stderr)
    -v {DEBUG,INFO,WARNING,ERROR}, --log-level {DEBUG,INFO,WARNING,ERROR}
                            Level of log information to output. Default is 'INFO'.
    ````

4. Any filelist files that were generated in the previous step must be copied to HDFS to be accessible in the `distcp` job. Use the following command to copy the files:

    ```bash
    hadoop fs -copyFromLocal {filelist_pattern} /[hdfs directory]
    ```

5. When using `distcp` to copy files from the on-premise Hadoop cluster to the Data Box, some directories will need to be excluded (they generally contain state information to keep the cluster running and so are not important to copy). The `distcp` tool supports a mechanism to exclude files & directories by specifying a series of regular expressions (1 per line) that exclude matching paths. On the on-premise Hadoop cluster where you will be initiating the `distcp` job, create a file with the list of directories to exclude, similar to the following:

    ```
    .*ranger/audit.*
    .*/hbase/data/WALs.*
    ```

6. Create the storage container that you want to use for data copy. You should also specify a destination folder as part of this command. This could be a dummy destination folder at this point.

    ```
    hadoop fs [-libjars $azjars] \
    -D fs.AbstractFileSystem.wasb.Impl=org.apache.hadoop.fs.azure.Wasb \
    -D fs.azure.account.key.{databox_blob_service_endpoint}={account_key} \
    -mkdir -p  wasb://{container_name}@{databox_blob_service_endpoint}/[destination_folder]
    ```

7. Run a list command to ensure that your container and folder were created.

    ```
    # hadoop fs [-libjars $azjars] \
    -D fs.AbstractFileSystem.wasb.Impl=org.apache.hadoop.fs.azure.Wasb \
    -D fs.azure.account.key.{databox_blob_service_endpoint}={account_key} \
    -ls -R  wasb://{container_name}@{databox_blob_service_endpoint}/
    ```

8. Run the following [distcp](https://hadoop.apache.org/docs/current/hadoop-distcp/DistCp.html) job to copy data and metadata from HDFS to Data Box. Note that we need to elevate to HDFS super-user permissions to avoid missing data due to lack of permissions:

    ```bash
    sudo -u hdfs \
    hadoop distcp [-libjars $azjars] \
    -D fs.AbstractFileSystem.wasb.Impl=org.apache.hadoop.fs.azure.Wasb \
    -D fs.azure.account.key.{databox_blob_service_endpoint}={account_key} \
    -filters {exclusion_filelist_file} \
    [-f filelist_file | /[source directory]] wasb://{container_name}@{databox_blob_service_endpoint}/[path]
    ```

   The following example shows how the `distcp` command is used to copy data.
   
    ```
    sudo -u hdfs \
    hadoop distcp -libjars $azjars \
    -D fs.AbstractFileSystem.wasb.Impl=org.apache.hadoop.fs.azure.Wasb \
    -D fs.azure.account.key.mystorageaccount.blob.mydataboxno.microsoftdatabox.com=myaccountkey \
    -filter ./exclusions.lst -f /tmp/copylist1 -m 4
    wasb://hdfscontainer@mystorageaccount.blob.mydataboxno.microsoftdatabox.com/data
   ```
  
    To improve the copy speed:
    - Try changing the number of mappers. (The above example uses `m` = 4 mappers.)
    - Try running mutliple `distcp` in parallel.
    - Remember that large files perform better than small files.       

## Step 3 - Ship the Data Box to Microsoft

Follow these steps to prepare and ship the Data Box device to Microsoft.

1. After the data copy is complete, run [Prepare to ship](https://docs.microsoft.com/azure/databox/data-box-deploy-copy-data-via-rest) on your Data Box. After the device preparation is complete, download the BOM files. You will use these BOM or manifest files later to verify the data uploaded to Azure. Shut down the device and remove the cables. 
2.	Schedule a pickup with UPS to [Ship your Data Box back to Azure](https://docs.microsoft.com/azure/databox/data-box-deploy-picked-up). 
3.	After Microsoft receives your device, it is connected to the network datacenter and data is uploaded to the storage account you specified (with Hierarchical Namespace disabled) when you ordered the Data Box. Verify against the BOM files that all your data is uploaded to Azure. You can now move this data to a Data Lake Storage Gen2 storage account.

## Move the data onto your Data Lake Storage Gen2 storage account

To most efficiently perform analytics operations on your data in Azure, you will need to copy the data to a storage account with the Hierarchical Namespace enabled - an Azure Data Lake Storage Gen2 account.

You can do this in 2 ways. 

- Use [Azure Data Factory to move data to ADLS Gen2](https://docs.microsoft.com/azure/data-factory/load-azure-data-lake-storage-gen2). You will have to specify **Azure Blob Storage** as the source.

- Use your Azure-based Hadoop cluster. You can run this DistCp command:

    ```bash
    hadoop distcp -Dfs.azure.account.key.{source_account}.dfs.windows.net={source_account_key} abfs://{source_container}@{source_account}.dfs.windows.net/[path] abfs://{dest_container}@{dest_account}.dfs.windows.net/[path]
    ```

This command copies both data and metadata from your storage account into your Data Lake Storage Gen2 storage account.

## Step 5 - Copy and map identities and permissions from HDFS to ADLS Gen2

1. On the on-premise Hadoop cluster, execute the following Bash command to generate a list of copied 
files with their permissions (depending on the number of files in HDFS, this command may take a long time to run):

    ```bash
    sudo -u hdfs ./copy-acls.sh -s /[hdfs_path] > ./filelist.json
    ```

2. Generate the list of unique identities that need to be mapped to AAD-based identities:

    ```bash
    ./copy-acls.py -s ./filelist.json -i id_map.json -g
    ```

3. Using a text editor open the generated `id_map.json` file. For each JSON object in the file, update the `target` attribute (either an AAD User Principal Name (UPN) or objectId (OID)) with the mapped identity. Once complete save the file for use in the next step.

4. Run the following script to apply permissions to the copied data in the ADLS Gen2 account. Note that the credentials for the service principal created during the Step 1 above should be specified here:

    ```bash
    ./copy-acls.py -s ./filelist.json -i ./id_map.json  -A adlsgen2hnswestus2 -C databox1 --dest-spn-id {spn_client_id} --dest-spn-secret {spn_secret}
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
