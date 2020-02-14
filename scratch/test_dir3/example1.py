#! /usr/bin/env python
import argparse,os,subprocess
from wpipe import *
import time


def register(PID,task_name):
   myPipe = Store().select('pipelines').loc[int(PID)]
   myTask = Task(task_name,myPipe).create()
   _t = Task.add_mask(myTask,'*','start',task_name)
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
      try:
         print(args.PID)
      except:
         print("Need to define a pipeline ID")
         exit
      myPipe = Pipeline.get(args.PID)
      myTarget = Target(name='start_targ', pipeline=myPipe).create(create_dir=True)
      _c = Configuration(name='test_config',target=myTarget).create(create_dir=True)
      config_id = _c['config_id'].values[0]
      myConfig = Configuration.get(config_id)
      params={'a':0,'x':12,'note':'testing this'}
      myParams = Parameters(params).create(myConfig)
      Parameters.addParam(int(myConfig.config_id),'annulus',12)
      Parameters.addParam(int(myConfig.config_id),'delta_annulus',8)
      _job = Job(config=myConfig).create()
      _event = Job.getEvent(_job)
      job_id = int(_job.job_id)
      event_id = int(_event.event_id)
      #job_id = int(args.job_id) #initial job makes its own job
      #event_id = int(args.event_id) #initial job makes its own event
      myJob = Job.get(job_id)
      logprint(myConfig,myJob,"test logging")
      to_run1 = 4
      to_run2 = 3
      testname1 = 'name1'
      testname2 = 'name2'
      comp_name1 = 'completed'+testname1
      comp_name2 = 'completed'+testname2
      
      options = {comp_name1:0, comp_name2:0}

      _opt = Options(options).create('job',job_id)
      
      for i in range(to_run1):
         event = Job.getEvent(myJob,'example1_done',options={'to_run':to_run1,'name':testname1})
         #print(event.event_id.values[0])
         #_job = Job(event_id=int(event.event_id)).create()
         #print("EXAM ",event['event_id'].values[0],"DONE")
         fire(event)
         #print("Event=",int(event.event_id),"; Job=",int(_job.job_id))
      for i in range(to_run2):
         event = Job.getEvent(myJob,'example1_done',options={'to_run':to_run2,'name':testname2})
         #_job = Job(event_id=int(event.event_id)).create()
         fire(event)
         #print("Event=",int(event.event_id),"; Job=",int(_job.job_id))

      
