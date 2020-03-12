#! /usr/bin/env python
import argparse

import wpipe as wp
import numpy as np


def register(task):
    _temp = task.mask(source='*', name='start', value=task.name)


def parse_all():
    parser = wp.PARSER
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
       register(wp.SQLPipeline(int(args.PID)).task(name=str(args.task_name)))
   else:
      if args.config_id is None:
         exit("Need to define a configuration ID")
      config_id = args.config_id

      # Fetch, update params
      params = wp.Parameters.getParam(config_id)
      params['oversample'] =4 


      # Fetch old config and the target
      oldConfig = wp.Configuration.get(config_id)
      target = wp.Target.get(oldConfig.target_id)
      target.relativepath = np.array([target.relativepath])

      # Create new config object in memory
      newConfig = wp.Configuration('os4',target=target)
      newConfig.config_id = np.array([0])

      # Write to db, create directories
      newConfig = newConfig.create(params=params,create_dir=True)
      print("OLD: ",oldConfig,"\n","New: ",newConfig) 
   # placeholder for additional steps
   print('done')

