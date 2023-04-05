#! /usr/bin/env python
import argparse

# from wpipe.scheduler import pbsconsumer
from wpipe.scheduler import base_consumer


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    parser_start = subparsers.add_parser('check', add_help=False)
    parser_start.set_defaults(which='check')

    parser_start = subparsers.add_parser('start', add_help=False)
    parser_start.set_defaults(which='start')

    parser_stop = subparsers.add_parser('stop', add_help=False)
    parser_stop.set_defaults(which='stop')

    args = parser.parse_args()
    if hasattr(args, 'which'):
        # pbsconsumer(args.which)
        base_consumer(args.which, kind='Pbs')
    else:
        parser.print_help()
