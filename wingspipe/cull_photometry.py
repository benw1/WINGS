#! /usr/bin/env python
import argparse

import wpipe as wp


def register(task):
    _temp = task.mask(source='*', name='start', value='*')
    _temp = task.mask(source='test_wpipe.py', name='test', value='*')


def create_target(a=''):
   pass
   
    
def parse_all():
    parser = wp.PARSER
    parser.add_argument('--R','-R', dest='REG', action='store_true',
                        help='Specify to Register')
    parser.add_argument('--P','-p',type=int,  dest='PID',
                        help='Pipeline ID')
    parser.add_argument('--N','-n',type=str,  dest='task_name',
                        help='Name of Task to be Registered')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_all()
    if args.REG:
       register(wp.SQLPipeline(int(args.PID)).task(name=str(args.task_name)))
    else:
       config = pipeline
       targs,data,config = discover_targets()
       count = 0;
       for targ in targs:
          pass
          #create_target(targ,data[count,:],config)
          #count++
