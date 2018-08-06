#! /usr/bin/env python
import argparse,os,subprocess
from wpipe import *
import numpy as np
import time
#from stips.observation_module import ObservationModule

def register(PID,task_name):
   myPipe = Store().select('pipelines').loc[int(PID)]
   myTask = Task(task_name,myPipe).create()
   _t = Task.add_mask(myTask,'*','start',task_name)
   _t = Task.add_mask(myTask,'*','new_stips_input','*')
   return

def do_something(job_id,event_id):
   time.sleep(np.random.rand(1)*1.0)
   return

def parse_all():
    parser = argparse.ArgumentParser()
    parser.add_argument('--R','-R', dest='REG', action='store_true',
                        help='Specify to Register')
    parser.add_argument('--P','-p',type=int,  dest='PID',
                        help='Pipeline ID')
    parser.add_argument('--N','-n',type=str,  dest='task_name',
                        help='Name of Task to be Registered')
    parser.add_argument('--E','-e',type=int,  dest='event_id',
                        help='Event ID')
    parser.add_argument('--J','-j',type=int,  dest='job_id',
                        help='Job ID')
    parser.add_argument('--DP','-dp',type=int,  dest='dp_id',
                        help='Dataproduct ID')
    return parser.parse_args()

 
if __name__ == '__main__':
   args = parse_all()
   if args.REG:
      _t = register(int(args.PID),str(args.task_name))
   else:
      job_id = int(args.job_id)
      event_id = int(args.event_id)
      myJob = Job.get(job_id)
      do_something(job_id,event_id)
      event = Event.get(event_id)
      parent_job_id = int(event.job_id)
      name = Options.get('event',event_id)['name']
      compname = 'completed'+name
      update_option = int(Options.get('job',parent_job_id)[compname])
      update_option = update_option+1
      _update = Options.addOption('job',parent_job_id,compname,update_option)
      to_run = int(Options.get('event',event_id)['to_run'])
      completed = update_option
      if (completed>=to_run):
         event = Job.getEvent(myJob,'example2_done',options={'sub_branch':name})
         _job  = Job().create()
         #Event.fire(event)
         print('completed example2')
         print("Event=",int(event.event_id),"; Job=",int(_job.job_id))
