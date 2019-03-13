#!/bin/sh

REPLACEMENT_JAR=${1:-azure-storage-6.1.0.jar}
JAR_TO_REPLACE=${2:-azure-storage-7.0.0.jar}

# Process all the archives in HDFS (these are used when distributing jobs)
for ARCHIVE in $(hadoop fs -find / -name "*.tar.gz")
do
    OWNER=$(hadoop fs -stat %u $ARCHIVE)
    GROUP=$(hadoop fs -stat %g $ARCHIVE)
    PERMISSIONS=$(hadoop fs -stat %a $ARCHIVE)
    FILENAME=$(basename $ARCHIVE)
    hadoop fs -copyToLocal -f $ARCHIVE "/tmp/${FILENAME}"
    mkdir "/tmp/${FILENAME}.dir"
    tar -C "/tmp/${FILENAME}.dir" -zxf "/tmp/${FILENAME}"
    # Replace the jar file from the archive
    REPLACEMENT_FOUND=false
    for FOUND_JAR in $(find "/tmp/${FILENAME}.dir" -name "$JAR_TO_REPLACE")
    do
        cp ${REPLACEMENT_JAR} $(dirname "$FOUND_JAR")/
        rm "$FOUND_JAR"
        REPLACEMENT_FOUND=true
    done
    if $REPLACEMENT_FOUND; then
        # Rebuild archive
        tar -czf "/tmp/${FILENAME}" -C "/tmp/${FILENAME}.dir" .
        # Put the archive back into HDFS using the right owner's creds & restore perms
        sudo -u $OWNER hadoop fs -copyFromLocal -f "/tmp/${FILENAME}" "$ARCHIVE"
        sudo -u $OWNER hadoop fs -chgrp $GROUP "$ARCHIVE"
        sudo -u $OWNER hadoop fs -chmod $PERMISSIONS "$ARCHIVE"
    fi
    rm -rf "/tmp/${FILENAME}.dir"
    rm -f "/tmp/${FILENAME}"
done
# Process all local instances
for ARCHIVE in $(find / -name "*.tar.gz" -print0 | xargs -0 zgrep "$JAR_TO_REPLACE" | tr ":" "\n" | grep .tar.gz)
do
    FILENAME=$(basename $ARCHIVE)
    tar -C "/tmp/${FILENAME}.dir" -zxf "$ARCHIVE"
    for FOUND_JAR in $(find "/tmp/${FILENAME}.dir" -name "$JAR_TO_REPLACE")
    do
        cp ${REPLACEMENT_JAR} $(dirname "$FOUND_JAR")/
        rm "$FOUND_JAR"
        REPLACEMENT_FOUND=true
    done
    rm -rf "/tmp/${FILENAME}.dir"
done