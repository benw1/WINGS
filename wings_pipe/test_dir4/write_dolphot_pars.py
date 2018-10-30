#! /usr/bin/env python
import argparse,os,subprocess
from wpipe import *

def register(PID,task_name):
   myPipe = Store().select('pipelines').loc[int(PID)]
   myTask = Task(task_name,myPipe).create()
   _t = Task.add_mask(myTask,'*','start',task_name)
   _t = Task.add_mask(myTask,'*','images_prepped','*')
   return

def write_dolphot_pars(target,config,thisjob):
   logprint(thisconf,thisjob,''.join(["Could write dolphot pars now","\n"]))
                     
   
    
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
       job_id = int(args.job_id)
       event_id = int(args.event_id)
       event = Event.get(event_id)
       thisjob = Job.get(job_id)
       config = Configuration.get(int(event.config_id))
       tid = config.target_id
       target = Target.get(int(config.target_id))
       write_dolphot_pars(target,config,thisjob)
       newevent = Job.getEvent(thisjob,'parameters_written',options={'target_id':tid})
       #fire(newevent)
       logprint(config,thisjob,'parameters_written\n')
