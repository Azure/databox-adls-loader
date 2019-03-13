#!/usr/bin/env python

import sys, subprocess, logging, itertools, argparse

log = logging.getLogger(__name__)

def processDirectoryIntoUnits(sourceDir, unitSize, dirAllocations, unitsSpaceAvailable):
    log.info("Calculating directory sizes for '%s'", sourceDir)
    startIdx = len(dirAllocations)
    process = subprocess.Popen("hadoop fs -du -x '{0}'".format(sourceDir), stdout=subprocess.PIPE, shell=True)
    retcode = process.wait()
    if retcode == 0:
        dirAllocations += [{
            'path': line.split(None, 2)[2].rstrip(),
            'size': int(line.split()[0]),
            'unit': 0
        } for line in process.stdout]
        recurseDirs = []
        for dirIdx in range(startIdx, len(dirAllocations)):
            if dirAllocations[dirIdx]["size"] > unitSize:
                # We will recurse down the path after we've processed all other items
                # This item will remain in the array, but unassigned with unit == 0. This will be filtered when we project the filelists
                recurseDirs.append(dirAllocations[dirIdx]["path"])
            else:
                for unitIdx in range(0, len(unitsSpaceAvailable)):
                    if (unitsSpaceAvailable[unitIdx] >= dirAllocations[dirIdx]["size"]):
                        dirAllocations[dirIdx]["unit"] = unitIdx + 1
                        unitsSpaceAvailable[unitIdx] -= dirAllocations[dirIdx]["size"]
                        break
                else:
                    # Allocate new unit
                    unitsSpaceAvailable += [unitSize]
                    unitIdx = len(unitsSpaceAvailable) - 1
                    dirAllocations[dirIdx]["unit"] = unitIdx + 1
                    unitsSpaceAvailable[unitIdx] -= dirAllocations[dirIdx]["size"]
        for recurseDir in recurseDirs:
            if not processDirectoryIntoUnits(recurseDir, unitSize, dirAllocations, unitsSpaceAvailable):
                return False
        return True
    else:
        log.warning("Error calling Hadoop: %d", retcode)
        return False

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Calculate filelist of HDFS contents into Databox sized blocks")
    parser.add_argument('path', help="The base HDFS path to process.")
    parser.add_argument('-s', '--databox-size', default=109951162777600, type=int, help="The size of each Databox in Bytes.")
    parser.add_argument('-b', '--filelist-basename', default="filelist", help="The base name for the output filelists. Lists will be named basename1, basename2, ... .")
    parser.add_argument('-f', '--log-config', help="The name of a configuration file for logging.")
    parser.add_argument('-l', '--log-file', help="Name of file to have log output written to (default is stdout/stderr)")
    parser.add_argument('-v', '--log-level', default="INFO", choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], help="Level of log information to output. Default is 'INFO'.")
    args = parser.parse_known_args()[0]
    logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=getattr(logging, args.log_level.upper()), filename=args.log_file)
    log.info("Starting processing of HDFS contents into chunked file lists")

    log.info("Calculating space requirements for HDFS directories")
    dirAllocations = []
    unitsSpaceAvailable = []
    if processDirectoryIntoUnits(args.path, args.databox_size, dirAllocations, unitsSpaceAvailable):
        log.info("Writing file lists with basename: %s", args.filelist_basename)
        keyfunc = lambda x: x["unit"]
        for unit, dirs in itertools.groupby(sorted([dir for dir in dirAllocations if dir["unit"] != 0], key=keyfunc), keyfunc):
            with open("{0}{1}".format(args.filelist_basename, unit), "w+") as fp:
                fp.writelines([dir["path"] + '\n' for dir in dirs])

        log.info("Completed processing successfully")
    else:
        log.error("Filelist sizing failed")
    