#! /usr/bin/env python
import argparse,os,subprocess,json
from wpipe import *
import glob

def register(PID,task_name):
   myPipe = Pipeline.get(PID)
   myTask = Task(task_name,myPipe).create()
   _t = Task.add_mask(myTask,'*','start',task_name) 
   return

def discover_targets(Pipe,config_file,data_dir):
   myPipe = Pipe
   myTarget = Target(name='start_targ', pipeline=myPipe).create()
   targlist = get_targ_list(data_dir)
   _c = Configuration(name='test_config',target=myTarget).create()
   config_id = _c['config_id'].values[0]
   myConfig = Configuration.get(config_id)
   params={'a':0,'x':12,'note':'testing this'}
   myParams = Parameters(params).create(myConfig)
   _job = Job(config=myConfig).create() #need to create dummy job to keep track of events
   job_id = int(_job.job_id)
   myJob = Job.get(job_id)
   for targ in targlist:
      myTarget = Target(name=targ,pipeline=myPipe).create(create_dir=True)
      _params = json.load(open(config_file))[0]
      conffilename = config_file.split('/')[-1]
      confname = conffilename.split('.')[0]
      conf = Configuration(name=confname,
                              target=myTarget)\
                              .create(params=_params,create_dir=True)
      print('Target ',targ,' created with Configuration ',confname)
      system('cp',config_file,conf.confpath+'/')
      _dp = DataProduct(filename=conffilename,relativepath=myPipe.config_root,group='conf',configuration=conf).create()
      targetfiles = get_target_files(data_dir,targ)
      for files in targetfiles:
         system('cp',files,conf.rawpath)
         _dp = DataProduct(filename=files,relativepath=conf.rawpath,group='raw',configuration=conf).create()
         send(_dp,conf,myTarget,len(targetfiles),job_id)

def get_targ_list(data_dir):
   data  = os.listdir(data_dir)
   target_names= []
   for dat in data:
      # each prefix is a target
      checkname = dat.split('.')[:-1][0]
      if checkname in target_names:
         continue
      target_names.append(dat.split('.')[:-1][0])
   return target_names

def get_target_files(data_dir,targ):
   targfiles = glob.glob(data_dir+'/'+targ+'*')
   return targfiles
       
def send(dp,conf,target,total,job_id):
   filepath = _dp.relativepath+'/'+_dp.filename
   data = np.loadtxt(filepath)
   if 'type' in data[0,0]:
      targname = target.name.values[0]
      comp_name = 'completed'+targname
      options = {comp_name:0}
      _opt = Options(options).create('job',job_id)

      event = Job.getEvent(myJob,'new_stips_catalog',options={'to_run':total,'completed':0})

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
    parser.add_argument('--C','-c',type=str,  dest='config_file',
                        help='Configuration File Path')
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
      try:
         print(args.PID)
      except:
         print("Need to define a pipeline ID")
         exit
      try:
         print(args.config_file)
      except:
         print("Need to define a configuration file")
         exit
      try:
         print(args.data_dir)
      except:
         print("Need to define a directory with input star lists")
         exit
      myPipe = Pipeline.get(args.PID)
   
      discover_targets(myPipe,args.config_file,args.data_dir)

   # placeholder for additional steps
   print('done')
