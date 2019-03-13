#!/usr/bin/env python

import requests
import sys, subprocess, datetime, json, itertools, os.path, threading, argparse, logging
from adls_copy_utils import AdlsCopyUtils, OAuthBearerToken

log = logging.getLogger(__name__)

def add_identity_header(headers, identity_type, identity, header, identity_map):
    mapped_identity = AdlsCopyUtils.lookupIdentity(identity_type, identity, identity_map)
    if mapped_identity:
        headers[header] = mapped_identity

def map_acl_entry(entry, identity_map):
    # Format is [scope:][type]:[id]:[permissions]
    items = entry.split(":")
    id_idx = 1 if len(items) == 3 else 2
    if items[id_idx]:
        items[id_idx] = AdlsCopyUtils.lookupIdentity(items[id_idx - 1], items[id_idx], identity_map)
    return ":".join(items)

def apply_file_acls(account, container, token_handler, identity_map, work_queue):
    log = logging.getLogger(threading.currentThread().name)
    log.debug("Thread starting: %d", threading.currentThread().ident)
    while not work_queue.isDone():
        file = work_queue.nextItem()
        if file:
            try:
                filename = file["file"]
                if not filename:
                    filename = "/" 
                log.debug(filename)
                # Set owner & group
                mapped_acl = [map_acl_entry(entry, identity_map) for entry in file["acl"]]
                headers = {
                    "x-ms-version": AdlsCopyUtils.ADLS_REST_VERSION,
                    "content-length": "0",
                    "Authorization": token_handler.checkAccessToken(),
                    "x-ms-acl": ",".join(mapped_acl)
                }
                add_identity_header(headers, "user", file["owner"], "x-ms-owner", identity_map)
                add_identity_header(headers, "group", file["group"], "x-ms-group", identity_map)
                set_owner_url = "https://{0}.dfs.core.windows.net/{1}/{2}?action=setAccessControl".format(account, container, filename)
                log.debug(set_owner_url)
                log.debug(headers)
                with requests.patch(set_owner_url, headers=headers) as set_owner_request:
                    require_retry = False
                    throw_err = False
                    if not set_owner_request:
                        throw_err = True
                        err = set_owner_request.json()
                        if "error" in err and "code" in err["error"]:
                            # Allow 'PathNotFound' errors to fail silently
                            code = err["error"]["code"]
                            if code == "PathNotFound":
                                log.debug("Skipping missing file: %s", filename)
                                throw_err = False
                            elif code == "InvalidNamedUserOrNamedGroup":
                                err["m-ms-acl"] = ",".join(mapped_acl)
                                err["owner"] = headers["owner"]
                                err["group"] = headers["group"]
                    if not require_retry:
                        work_queue.itemDone()
                    if throw_err:
                        raise IOError(err)
            except IOError as e:
                log.warning("Failed to copy file: %s. Details: %s", filename, e.args)
    log.debug("Thread ending")

if __name__ == '__main__':
    parser = AdlsCopyUtils.createCommandArgsParser("Apply ACLs to ADLS account", False, (True, False))
    parser.add_argument('-g', '--generate-identity-map', action='store_true', help="Specify this flag to generate a based identity mapping file using the unique identities in the source account. The identity map will be written to the file specified by the --identity-map argument.")
    parser.add_argument('-s', '--source-acls', help="The filename containing the JSON definition. If omitted, input is read from stdin")
    args = parser.parse_known_args()[0]

    AdlsCopyUtils.configureLogging(args.log_config, args.log_level, args.log_file)
    print("Processing source ACLs")

    # Queue the items up
    source_fd = sys.stdin
    if args.source_acls:
        source_fd = open(args.source_acls)

    if args.generate_identity_map:
        log.info("Generating identity map from source account to file: " + args.identity_map)
        acls = json.load(source_fd)
        unique_users = set([item.split(":")[1] 
            for file in acls 
            for item in file["acl"]+["user:"+file["owner"]+":"] 
                if item.split(":")[0] == "user" and item.split(":")[1]]) 
        unique_groups = set([item.split(":")[1] 
            for file in acls 
            for item in file["acl"]+["group:"+file["group"]+":"] 
                if item.split(":")[0] == "group" and item.split(":")[1]]) 
        identities = [
            {
                "type": identity_type["type"],
                "source": identity,
                "target": ""
            } 
            for identity_type in [{"type": AdlsCopyUtils.IDENTITY_USER, "identities": unique_users}, {"type": AdlsCopyUtils.IDENTITY_GROUP, "identities": unique_groups}]
            for identity in identity_type["identities"]]
        with open(args.identity_map, "w+") as f:
            json.dump(identities, f)
    else:
        # Load identity map
        identity_map = AdlsCopyUtils.loadIdentityMap(args.identity_map)

        if not args.dest_account or not args.dest_container:
            parser.print_help()
            parser.exit()

        # OAuth token handler
        token_handler = OAuthBearerToken(args.dest_spn_id, args.dest_spn_secret)

        log.info("Applying ACLs in destination")
        AdlsCopyUtils.processWorkQueue(apply_file_acls,
            [args.dest_account, args.dest_container, token_handler, identity_map],
            json.load(source_fd),
            args.max_parallelism)

    print("All work processed. Exiting")
