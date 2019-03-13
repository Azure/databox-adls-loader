#!/usr/bin/env bash

source_path=""

noop1() {
    echo $1
}

process_acl_entries() {
    source_path=$1
    is_relative_path=$(if [[ ${source_path:0:1} == "/" ]] ; then echo 1; else echo 0; fi)

    while read file; do
        if (( $is_relative_path )); then
            file=$(echo $file | cut -d / -f 2-)
        else
            file=$(echo $file | cut -d / -f 4-)
        fi
        aclspec=()
        owner=""
        group=""
        while true
        do
            read identity
            if [[ ${identity:0:1} != '#' ]] 
            then
                aclentry=$identity
                break
            fi
            ownertype=$(echo $identity | cut -d ':' -f 1 | cut -c 3-)
            identity=$(echo $identity | cut -d ':' -f 2 | sed -e 's/^[ \t]*//')
            if [[ $ownertype == "owner" ]]
            then
                owner=$identity
            elif [[ $ownertype == "group" ]]
            then
                group=$identity
            fi
        done
        while [[ $aclentry ]]
        do
            aclspec+=($(echo $aclentry | cut -d "#" -f 1))
            read aclentry
        done
        echo "'$file'" "'$owner'" "'$group'" "${aclspec[@]}"
    done < <(hadoop fs -Dfs.azure.localuserasfileowner.replace.principals= -getfacl -R $source_path)
}

while getopts "s:" option; 
do
    case "${option}" in
        s)
            source_path=${OPTARG}
            ;;
    esac
done
if [[ -z $source_path ]]
then
    echo "Usage: $0 {-s source_path}" >&2
    exit 1
fi

echo "Copying ACLs from $source_path" >&2
process_acl_entries $source_path | jq -R 'split(" ") | {file:.[0], owner:.[1], group:.[2], acl:.[3:]}' | jq -s '.' | tr -d "'" 
