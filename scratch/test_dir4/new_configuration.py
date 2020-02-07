#! /usr/bin/env python
import argparse,os,subprocess,json
from wpipe import *
import glob

def register(PID,task_name):
   myPipe = Pipeline.get(PID)
   myTask = Task(task_name,myPipe).create()
   _t = Task.add_mask(myTask,'*','start',task_name) 
   return

def parse_all():
    parser = argparse.ArgumentParser()
    parser.add_argument('--R','-R', dest='REG', action='store_true',
                        help='Specify to Register')
    parser.add_argument('--N','-n',type=str,  dest='task_name',
                        help='Name of Task to be Registered')
    parser.add_argument('--P','-p',type=int,  dest='PID',
                        help='Pipeline ID')
    parser.add_argument('--C','-c',type=str,  dest='config_id',
                        help='Configuration ID to be copied')
    parser.add_argument('--T','-t',type=str,  dest='data_dir',
                        help='Path to directory with input lists')
    parser.add_argument('--E','-e',type=int,  dest='event_id',
                        help='Event ID')
    parser.add_argument('--J','-j',type=int,  dest='job_id',
                        help='Job ID')
    return parser.parse_args()

 
if __name__ == '__main__':
   args = parse_all()
   if args.REG:
      _t = register(int(args.PID),str(args.task_name))
   else:
      if args.config_id is None:
         exit("Need to define a configuration file")
      config_id = args.config_id

      # Fetch, update params
      params = Parameters.getParam(config_id)
      params['over'] = 1


      # Fetch old config and the target
      oldConfig = Configuration.get(config_id)
      target = Target.get(oldConfig.target_id)
      target.relativepath = np.array([target.relativepath])

      # Create new config object in memory
      newConfig = Configuration('os1',target=target)
      newConfig.config_id = np.array([0])

      # Write to db, create directories
      newConfig = newConfig.create(params=params,create_dir=True)
      print("OLD: ",oldConfig,"\n","New: ",newConfig) 
   # placeholder for additional steps
   print('done')

