#! /usr/bin/env python
import argparse,os,subprocess
from wpipe import *

from pipebackbone import Store

def register(PID,task_name):
   myPipe = Store().select('pipelines').loc[int(PID)]
   myTask = Task(task_name,myPipe).create()
   _t = Task.add_mask(myTask,'*','start','*')
   _t = Task.add_mask(myTask,'test_wpipe.py','test','*')
   return

def create_target(a=''):
   pass
   
    
def parse_all():
    parser = argparse.ArgumentParser()
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
       _t = register(args.PID,args.task_name)
    else:
       config = pipeline
       targs,data,config = discover_targets()
       count = 0;
       for targ in targs:
          pass
          #create_target(targ,data[count,:],config)
          #count++
