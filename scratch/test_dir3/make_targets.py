#! /usr/bin/env python
import argparse,os,subprocess,json
from wpipe import *

def register(PID,task_name):
   myPipe = Pipeline.get(PID)
   myTask = Task(task_name,myPipe).create()
   _t = Task.add_mask(myTask,'*','start',task_name) 
   return

def discover_targets(job_id,event_id):
   myJob = Job.get(job_id)
   myPipe = Pipeline.get(int(myJob.pipeline_id))
   start_targ = Target(name='unsorted',
                       pipeline=myPipe).create(create_dir=True)
   _params = json.load(open('config/default.conf'))[0]
   start_conf = Configuration(name='default',
                              target=start_targ)\
                              .create(params=_params,create_dir=False)
   _dp = DataProduct(filename='default.conf',
                     relativepath=myPipe.config_root,
                     group='conf',
                     configuration=start_conf)\
                     .create()
   data  = os.listdir(start_targ.relativepath)
   target_names= []
   for dat in data:
      # each raw catalog is a target
      target_names.append(dat.split('.')[:-1][0])
      _dp = DataProduct(filename=dat,
                        relativepath=start_targ.relativepath,
                        group='raw',
                        configuration=start_conf)\
                        .create()
   for targ in target_names:
      create_target(targ,myPipe,myJob)

   # This increments "completed" by 1
   Event.run_complete(Event.get(int(event_id)))

   _parent = Options.get('event',event_id)
   to_run,completed = int(_parent['to_run']), int(_parent['completed'])

   if !(completed<to_run):
      event = Job.getEvent(myJob,'make_targets_completed')
      
   return None
       
def create_target(targ,myPipe,myJob):
   new_target = Target(name=targ,
                       pipeline=pipeline)\
                       .create(create_dir=True)

   _params = json.load(open('config/default.conf'))[0]
   _params['total_data']=1
   _params['data_processed']=0

   # stored in 'database', not a physical file in target/conf
   new_conf = Configuration(name='new',
                            target=new_target)\
                            .create(params=_params,create_dir=True)

   _t = subprocess.call(['cp',pipeline.data_root+'/Unsorted/'+targ+'.*',
                         new_target.relativepath+'/raw_'+str(new_conf['name']),
                         stdout=subprocess.PIPE])
   
   dat = os.listdir(new_target.relativepath+'/raw_'+str(new_conf['name']))[0]
   _dp = DataProduct(filename=dat,
                     relativepath=new_target.relativepath+'/raw'+str(new_conf['name']),
                     group='raw', subtype='catalog',
                     configuration=new_conf)\
                     .create()
   event = Job.getEvent(myJob,'new_catalog',options={'dp_id': [int(_dp.dp_id)],
                                                     'to_run':1,
                                                     'completed':0})

   # Not implemented yet
   # return Event.fire(event)
   return None
   
    
def parse_all():
    parser = argparse.ArgumentParser()
    parser.add_argument('--R','-R', dest='REG', action='store_true',
                        help='Specify to Register')
    parser.add_argument('--N','-n',type=str,  dest='task_name',
                        help='Name of Task to be Registered')
    parser.add_argument('--P','-p',type=int,  dest='PID',
                        help='Pipeline ID')
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
      job_id = int(args.job_id)
      event_id = int(args.event_id)
      discover_targets(job_id,event_id)

   # placeholder for additional steps
   print('done')
