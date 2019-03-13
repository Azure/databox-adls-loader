#!/usr/bin/env python

import requests
import sys, subprocess, datetime, json, itertools, os.path, threading, argparse, logging
from adls_copy_utils import AdlsCopyUtils

log = logging.getLogger(__name__)

def update_files_owners(account, container, sas_token, work_queue):
    log = logging.getLogger(threading.currentThread().name)
    log.debug("Thread starting: %d", threading.currentThread().ident)
    while not work_queue.isDone():
        file = work_queue.nextItem()
        if file:
            file["permissions"]["owner"] = AdlsCopyUtils.lookupIdentity(AdlsCopyUtils.IDENTITY_USER, file["permissions"]["owner"], identity_map)
            file["permissions"]["group"] = AdlsCopyUtils.lookupIdentity(AdlsCopyUtils.IDENTITY_GROUP, file["permissions"]["group"], identity_map)
            # Merge the updated information into the other metadata properties, so that we can update in 1 call
            file["metadata"][AdlsCopyUtils.METDATA_PERMISSIONS] = json.dumps(file["permissions"])
            if file["is_folder"]:
                file["metadata"][AdlsCopyUtils.METADATA_ISFOLDER] = "true"
            url = "http://{0}.blob.core.windows.net/{1}/{2}?comp=metadata&{3}".format(account, container, file["name"], sas_token)
            log.debug(url)
            # No portable way to combine 2 dicts
            metadata_headers = {
                "x-ms-version": "2018-03-28",
                "x-ms-date": datetime.datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
            }
            metadata_headers.update({"x-ms-meta-" + name: value for (name, value) in file["metadata"].items()})
            with requests.put(url, headers=metadata_headers) as response:
                if not response:
                    log.warning("Failed to set metadata on file: %s. Error: %s", url, response.text)
                else:
                    work_queue.itemDone()
                    log.debug("Updated ownership for %s", file["name"])
    log.debug("Thread ending")

if __name__ == '__main__':
    parser = AdlsCopyUtils.createCommandArgsParser("Remaps identities on HDFS sourced data")
    parser.add_argument('-g', '--generate-identity-map', action='store_true', help="Specify this flag to generate a based identity mapping file using the unique identities in the source account. The identity map will be written to the file specified by the --identity-map argument.")
    args = parser.parse_known_args()[0]

    AdlsCopyUtils.configureLogging(args.log_config, args.log_level, args.log_file)
    print("Remapping identities for file owners in account: " + args.source_account)

    # Acquire SAS token, so that we don't have to sign each request (construct as string as Python 2.7 on linux doesn't marshall the args correctly with shell=True)
    sas_token = AdlsCopyUtils.getSasToken(args.source_account, args.source_key)

    # Get the full account list 
    inventory = AdlsCopyUtils.getSourceFileList(args.source_account, args.source_key, args.source_container, args.prefix)
    
    if args.generate_identity_map:
        log.info("Generating identity map from source account to file: " + args.identity_map)
        unique_users = set([x["permissions"]["owner"] for x in inventory])
        unique_groups = set([x["permissions"]["group"] for x in inventory])
        identities = [{
            "type": identity_type["type"],
            "source": identity,
            "target": ""
        } for identity_type in [{"type": AdlsCopyUtils.IDENTITY_USER, "identities": unique_users}, {"type": AdlsCopyUtils.IDENTITY_GROUP, "identities": unique_groups}]
        for identity in identity_type["identities"]]
        with open(args.identity_map, "w+") as f:
            json.dump(identities, f)
    else:
        # Load identity map
        identity_map = AdlsCopyUtils.loadIdentityMap(args.identity_map)
        # Fire up the processing in args.max_parallelism threads, co-ordinated via a thread-safe queue
        AdlsCopyUtils.processWorkQueue(update_files_owners, [args.source_account, args.source_container, sas_token], inventory, args.max_parallelism)
    print("All work processed. Exiting")

