#!/usr/bin/env python

import requests
import subprocess, datetime, json, itertools, os.path, threading, argparse, logging
from adls_copy_utils import AdlsCopyUtils, OAuthBearerToken

BLOCK_SIZE = 20 * pow(2, 20)

log = logging.getLogger(__name__)

def add_identity_header(headers, identity_type, identity, header, identity_map):
    mapped_identity = AdlsCopyUtils.lookupIdentity(identity_type, identity, identity_map)
    if mapped_identity:
        headers[header] = mapped_identity
    else:
        # TODO: Lookup identity in AAD
        pass

def create_adls_resource(account, container, resource_type, resource, token_handler, identity_map):
    resource_uri = "https://{0}.dfs.core.windows.net/{1}/{2}?resource={3}".format(account, container, resource["name"], resource_type)
    log.debug(resource_uri)
    create_request = requests.put(resource_uri,
        headers = {
            "x-ms-version": AdlsCopyUtils.ADLS_REST_VERSION,
            "content-length": "0", 
            "x-ms-permissions": resource["permissions"]["permissions"],
            "x-ms-umask": "0000",
            "Authorization": token_handler.checkAccessToken()
        })
    if create_request:
        # Set owner & group
        headers = {
            "x-ms-version": AdlsCopyUtils.ADLS_REST_VERSION,
            "content-length": "0",
            "Authorization": token_handler.checkAccessToken()
        }
        add_identity_header(headers, "user", resource["permissions"]["owner"], "x-ms-owner", identity_map)
        add_identity_header(headers, "group", resource["permissions"]["group"], "x-ms-group", identity_map)
        set_owner_url = "https://{0}.dfs.core.windows.net/{1}/{2}?action=setAccessControl".format(account, container, resource["name"])
        log.debug(set_owner_url)
        log.debug(headers)
        set_owner_request = requests.patch(set_owner_url, headers=headers)
        if not set_owner_request:
            raise IOError(set_owner_request.json())
    else:
        raise IOError(create_request.json())

def copy_files(source_account, source_container, dest_account, dest_container, sas_token, token_handler, identity_map, work_queue):
    log = logging.getLogger(threading.currentThread().name)
    log.debug("Thread starting: %d", threading.currentThread().ident)
    while not work_queue.isDone():
        file = work_queue.nextItem()
        if file:
            try:
                log.debug(file["name"])
                # Create the destination file
                create_adls_resource(dest_account, 
                    dest_container,
                    "file",
                    file,
                    token_handler,
                    identity_map)
                # Copy the file, 20MB chunks at a time
                source_url = "http://{0}.blob.core.windows.net/{1}/{2}?{3}".format(source_account, source_container, file["name"], sas_token)
                dest_base_url = "https://{0}.dfs.core.windows.net/{1}/{2}?".format(dest_account, dest_container, file["name"])
                for offset in range(0, file["length"], BLOCK_SIZE):
                    source_request = requests.get(source_url, 
                        headers = {
                            "x-ms-range": "bytes={0}-{1}".format(offset, offset + BLOCK_SIZE - 1)
                        }, 
                        stream=True)
                    if source_request:
                        source_request.raw.decode_content = True
                        source_request.raw.__dict__["len"] = int(source_request.headers["Content-Length"])
                        dest_request = requests.patch(dest_base_url + "action=append&position=" + str(offset), 
                            headers = {
                                "Authorization": token_handler.checkAccessToken()
                            },
                            data = source_request.raw)
                        if not dest_request:
                            raise IOError(dest_request.json())
                    else:
                        raise IOError(source_request.json())
                # Flush the file
                dest_request = requests.patch(dest_base_url + "action=flush&position=" + str(file["length"]),
                    headers={
                        "content-length": "0",
                        "Authorization": token_handler.checkAccessToken()
                    })
                if not dest_request:
                    raise IOError(dest_request.json())

                work_queue.itemDone()
            except IOError as e:
                log.warning("Failed to copy file: %s. Details: %s", file["name"], e.args)
    log.debug("Thread ending")

if __name__ == '__main__':
    parser = AdlsCopyUtils.createCommandArgsParser("Remaps identities on HDFS sourced data", add_dest_args=True)
    args = parser.parse_known_args()[0]

    AdlsCopyUtils.configureLogging(args.log_config, args.log_level, args.log_file)
    print("Copying directories, files and permissions from account: " + args.source_account + " to: " + args.dest_account)

    # OAuth token handler
    token_handler = OAuthBearerToken(args.dest_spn_id, args.dest_spn_secret)

    # Acquire SAS token, so that we don't have to sign each request (construct as string as Python 2.7 on linux doesn't marshall the args correctly with shell=True)
    sas_token = AdlsCopyUtils.getSasToken(args.source_account, args.source_key)

    # Get the full account list 
    inventory = AdlsCopyUtils.getSourceFileList(args.source_account, args.source_key, args.source_container, args.prefix)
    
    # Load identity map
    identity_map = AdlsCopyUtils.loadIdentityMap(args.identity_map)

    # Create the directories first
    log.info("Creating directory structure in destination")
    for directory in [x for x in inventory if x["is_folder"]]:
        dir_base_url = "https://{0}.dfs.core.windows.net/{1}/{2}".format(args.dest_account, args.dest_container, directory["name"])
        create_adls_resource(args.dest_account, 
            args.dest_container,
            "directory",
            directory,
            token_handler,
            identity_map)
    # Now copy the files in parallel
    log.info("Copying files from source to destination")
    AdlsCopyUtils.processWorkQueue(copy_files, 
        [args.source_account, args.source_container, args.dest_account, args.dest_container, sas_token, token_handler, identity_map], 
        [file for file in inventory if not file["is_folder"]], 
        args.max_parallelism)

    print("All work processed. Exiting")
