#! /usr/bin/env python
import os
import argparse
import subprocess
from wpipe.scheduler.PbsConsumer import sendJobToPbs
from wpipe.scheduler.PbsConsumer import checkPbsConnection


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    parser_start = subparsers.add_parser('start', add_help=False)
    parser_start.set_defaults(which='start')

    parser_stop = subparsers.add_parser('stop', add_help=False)
    parser_stop.set_defaults(which='stop')

    args = parser.parse_args()
    if hasattr(args, 'which'):
        if args.which == 'start':
            connection = checkPbsConnection()
            if connection != 0:
                print("Starting PbsConsumer ...")
                homedir = os.path.expanduser('~/.pbsconsumer')
                if not os.path.exists(homedir):
                    os.mkdir(homedir)
                elif not os.path.isdir(homedir):
                    raise FileExistsError("%s is not a directory" % homedir)
                subprocess.Popen(["nohup", "python", "-m", "wpipe.scheduler.PbsConsumer"], cwd=homedir)
            else:
                print("PbsConsumer is already running ...")
        else:
            connection = checkPbsConnection()
            if connection == 0:
                if args.which == 'stop':
                    print("Shutting down PbsConsumer ...")
                    sendJobToPbs('poisonpill')
                elif args.which == 'log':
                    print("Printing current PbsConsumer log ...")
                    # TODO
            else:
                print("No server found, nothing to do ...")

    else:
        parser.print_help()
