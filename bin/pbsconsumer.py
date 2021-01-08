#! /usr/bin/env python
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
                subprocess.Popen(["python", "-m", "wpipe.scheduler.PbsConsumer"])
            else:
                print("PbsConsumer is already running ...")
        elif args.which == 'stop':
            connection = checkPbsConnection()
            if connection == 0:
                print("Shutting down PbsConsumer ...")
                sendJobToPbs('poisonpill')
            else:
                print("No server found, nothing to do ...")

    else:
        parser.print_help()
