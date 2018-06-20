#! /usr/bin/env python
import argparse,os,subprocess
from wpipe import *

def register(PID,task_name):
   myPipe = Store().select('pipelines').loc[int(PID)]
   myTask = Task(task_name,myPipe).create()
   _t = Task.add_mask(myTask,'*','start','run_stips.py')
   _t = Task.add_mask(myTask,'*','new_stips_input','*')
   return

def run_stips(data,configuration,parent_job,total_in,processed):
   pass
   
    
def parse_all():
    parser = argparse.ArgumentParser()
    parser.add_argument('--R','-R', dest='REG', action='store_true',
                        help='Specify to Register')
    parser.add_argument('--P','-p',type=int,  dest='PID',
                        help='Pipeline ID')
    parser.add_argument('--E','-e',type=int,  dest='event_id',
                        help='Event ID')
    parser.add_argument('--J','-j',type=int,  dest='job_id',
                        help='Job ID')
    parser.add_argument('--N','-n',type=str,  dest='task_name',
                        help='Name of Task to be Registered')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_all()
    if args.REG:
       _t = register(args.PID,args.task_name)
    else:
       event = Source().Event(event_id)
       target_id = event.getValue('target_id')
       data_id = event.getValue('data_id')
       data = Source().DataProduct(data_id)
       parent_job_id = event.getValue('parent_job_id') #events should all know parent job id
       configuration_id = event.getValue('configuration_id') #events should all know their parent configuration
       configuration = Source().Configuration(configuration_id)
       parent_job = Source().Job(parent_job_id)
       total_in = event.getValue('total_stips_in')
       processed = event.getVault('processed')
       run_stips(data,configuration,parent_job,total_in,processed)
