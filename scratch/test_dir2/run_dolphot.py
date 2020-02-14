#! /usr/bin/env python
import argparse,os,subprocess
from wpipe import *
from shutil import which

def register(PID,task_name):
   myPipe = Pipeline.get(PID)
   myTask = Task(task_name,myPipe).create()
   _t = Task.add_mask(myTask,'*','start','*')
   _t = Task.add_mask(myTask,'*','parameters_written','*')
   return

def run_dolphot(job_id,event_id,dp_id):
   myJob  = Job.get(job_id)
   myPipe = Pipeline.get(int(myJob.pipeline_id))

   paramDP = DataProduct.get(int(dp_id))
   myTarget = Target.get(int(paramDP.target_id))
   myConfig = Configuration.get(int(paramDP.config_id))
   myParams = Parameters.getParam(int(myConfig.config_id))
   targname = myTarget['name']
   outfile = targname+'.phot'
   logfile = outfile+".log"
   dolphot_path = which('wfirstmask')
   dolphot_path = dolphot_path[:-10]
   dolphot = dolphot_path+"dolphot"
   parameter_file = paramDP.relativepath+'/'+paramDP.filename
   subprocess.run([dolphot,outfile," -p"+parameter_file," >",myConfig.logpath+'/'+logfile],cwd=myConfig.procpath)
   

def hyak_dolphot(job_id,event_id,dp_id):
   myJob  = Job.get(job_id)
   myPipe = Pipeline.get(int(myJob.pipeline_id))

   paramDP = DataProduct.get(int(dp_id))
   parameter_file = paramDP.relativepath+'/'+paramDP.filename
   myTarget = Target.get(int(paramDP.target_id))
   myConfig = Configuration.get(int(paramDP.config_id))
   myParams = Parameters.getParam(int(myConfig.config_id))
   targname = myTarget['name']
   outfile = targname+'.phot'
   logfile = outfile+".log"
   dolphot_path = which('wfirstmask')
   dolphot_path = dolphot_path[:-10]
   dolphot = dolphot_path+"dolphot"

   slurmfile = parameter_file+'.slurm'
   with open(slurmfile, 'w') as f:
      f.write('#!/bin/bash' + '\n'+
              '## Job Name' + '\n'+
              '#SBATCH --job-name=dolphot_'+str(dp_id)+ '\n'+
              '## Allocation Definition ' + '\n'+
              '#SBATCH --account=astro' + '\n'+
              '#SBATCH --partition=astro' + '\n'+
              '## Resources' + '\n'+
              '## Nodes' + '\n'+
              '#SBATCH --ntasks=1' + '\n'+
              '## Walltime (3 hours)' + '\n'+
              '#SBATCH --time=100:00:00' + '\n'+
              '## Memory per node' + '\n'+
              '#SBATCH --mem=10G' + '\n'+
              '## Specify the working directory for this job' + '\n'+
              '#SBATCH --workdir='+myConfig.procpath + '\n'+
              '##turn on e-mail notification' + '\n'+
              dolphot+" "+outfile+" -p"+parameter_file+" >"+myConfig.logpath+'/'+logfile)
   subprocess.run(['sbatch',slurmfile],cwd=myConfig.procpath)

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
      _t = register(args.PID,args.task_name)
    else:
      job_id = int(args.job_id)
      event_id = int(args.event_id)
      event = Event.get(event_id)
      dp_id = Options.get('event',event_id)['dp_id']
      hyak_dolphot(job_id,event_id,dp_id)
      #run_dolphot(job_id,event_id,dp_id)
