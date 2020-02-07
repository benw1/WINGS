#! /usr/bin/env python
import time, subprocess, os
import numpy as np
import pandas as pd
pd.set_option('io.hdf.default_format','table')

from wings_pipe import Configuration, DataProduct, Job, \
    Parameters, Pipeline, Options, Store, Target, Task



def update_time(x):
    x.timestamp = pd.to_datetime(time.time(),unit='s')
    return x


def increment(df,x):
    df[x] = int(df[x])+1
    return df


def _min_itemsize(x):
    min_itemsize = {}
    for k,_dt in dict(x.dtypes).items():
        if _dt is np.dtype('O'):
            min_itemsize[k] = int(256)
    return min_itemsize


def Submit(task,job_id,event_id):
    pid = task.pipeline_id
    myPipe = Pipeline.get(pid)
    swroot = myPipe.software_root
    executable = swroot+'/'+task['name']
    dataroot = myPipe.data_root
    job = Job.get(int(job_id))
    #subprocess.Popen([executable,'-e',str(event_id),'-j',str(job_id)],cwd=dataroot) # This line will work with an SQL backbone, but NOT hdf5, as 2 tasks running on the same hdf5 file will collide!
    subprocess.run([executable,'-e',str(event_id),'-j',str(job_id)],cwd=dataroot)
    #Let's send stuff to slurm
    #hyak(task,job_id,event_id)
    #Let's send stuff to pbs
    #pbs(task,job_id,event_id)
    return

def hyak(task,job_id,event_id):
    myJob  = Job.get(job_id)
    myPipe = Pipeline.get(int(myJob.pipeline_id))
    swroot = myPipe.software_root
    executable = swroot+'/'+task['name']
    dataroot = myPipe.data_root

    catalogID = Options.get('event',event_id)['dp_id']
    catalogDP = DataProduct.get(int(catalogID))
    myTarget = Target.get(int(catalogDP.target_id))
    myConfig = Configuration.get(int(catalogDP.config_id))
    myParams = Parameters.getParam(int(myConfig.config_id))

    slurmfile = myConfig.confpath+'/'+task['name']+'_'+str(job_id)+'.slurm'
    #print(event_id,job_id,executable,type(executable))
    eidstr = str(event_id)
    jidstr = str(job_id)
    print("Submitting ",slurmfile)
    with open(slurmfile, 'w') as f:
        f.write('#!/bin/bash' + '\n'+
              '## Job Name' + '\n'+
              '#SBATCH --job-name='+jidstr+'\n'+
              '## Allocation Definition ' + '\n'+
              '#SBATCH --account=astro' + '\n'+
              '#SBATCH --partition=astro' + '\n'+
              '## Resources' + '\n'+
              '## Nodes' + '\n'+
              '#SBATCH --ntasks=1' + '\n'+
              '## Walltime (10 hours)' + '\n'+
              '#SBATCH --time=10:00:00' + '\n'+
              '## Memory per node' + '\n'+
              '#SBATCH --mem=10G' + '\n'+
              '## Specify the working directory for this job' + '\n'+
              '#SBATCH --workdir='+myConfig.procpath + '\n'+
              'source activate forSTIPS3'+'\n'+
              executable+' -e '+eidstr+' -j '+jidstr)
    subprocess.run(['sbatch',slurmfile],cwd=myConfig.confpath)


def pbs(task,job_id,event_id):
    myJob  = Job.get(job_id)
    myPipe = Pipeline.get(int(myJob.pipeline_id))
    swroot = myPipe.software_root
    executable = swroot+'/'+task['name']
    dataroot = myPipe.data_root

    catalogID = Options.get('event',event_id)['dp_id']
    catalogDP = DataProduct.get(int(catalogID))
    myTarget = Target.get(int(catalogDP.target_id))
    myConfig = Configuration.get(int(catalogDP.config_id))
    myParams = Parameters.getParam(int(myConfig.config_id))

    #pbsfile = myConfig.confpath+'/'+task['name']+'_'+str(job_id)+'.pbs'
    pbsfile = '/home1/bwilli24/Wpipelines/'+task['name']+'_jobs'

    #print(event_id,job_id,executable,type(executable))
    eidstr = str(event_id)
    jidstr = str(job_id)
    print("Submitting ",pbs)
    #with open(pbsfile, 'w') as f:
    with open(pbsfile, 'a') as f:
        f.write(#'#PBS -S /bin/csh' + '\n'+
                #'#PBS -j oe' + '\n'+
                #'#PBS -l select=1:ncpus=4:model=san' + '\n'+
                #'#PBS -W group_list=s1692' + '\n'+
                #'#PBS -l walltime=10:00:00' + '\n'+

                #'cd ' + myConfig.procpath  + '\n'+

                #'source activate STIPS'+'\n'+
              
                #executable+' -e '+eidstr+' -j '+jidstr)
                'source /nobackupp11/bwilli24/miniconda3/bin/activate STIPS && '+executable+' -e '+eidstr+' -j '+jidstr + '\n')

    subprocess.run(['qsub',pbsfile],cwd=myConfig.confpath)


def fire(event):
    event_name = event['name'].values[0]
    event_value = event['value'].values[0]
    event_id = event['event_id'].values[0]
    #print("HERE ",event['name'].values[0]," DONE")
    parent_job = Job.get(int(event.job_id))
    try:
        conf_id = int(Options.get('event',event_id)['config_id'])
    except:
        conf_id = int(parent_job.config_id)
    configuration = Configuration.get(conf_id)
    pipeline_id = parent_job.pipeline_id
    #print(pipeline_id)
    alltasks =  Store().select('tasks',where="pipeline_id=="+str(pipeline_id))
    for i in range(alltasks.shape[0]):
        task = alltasks.iloc[i]
        task_id = task['task_id']
        #print(task_id)
        m = Store().select('masks',where="task_id=="+str(task_id))
        for j in range(m.shape[0]):
            mask = m.iloc[j]
            mask_name = mask['name']
            mask_value = mask['value']
    
            #print("HERE",event_name,mask_name,event_value,mask_value,"DONE3")
            if (event_name == mask_name) & ((event_value == mask_value) | (mask_value=='*')):
                taskname = task['name']
                newjob = Job(task=task,config=configuration,event_id=event_id).create() #need to give this a configuration
                job_id = int(newjob['job_id'].values[0])
                event_id = int(event['event_id'].values[0])
                print(taskname,"-e",event_id,"-j",job_id)
                Submit(task,job_id,event_id) #pipeline should be able to run stuff and keep track if it completes
                return


def logprint(configuration,job,log_text):
    target_id = configuration['target_id']#.values[0]
    pipeline_id = configuration['pipeline_id']#.values[0]
    myPipe = Pipeline.get(pipeline_id)
    myTarg = Target.get(target_id)
    conf_name = configuration['name']#.values[0]
    targ_name = myTarg['name']
    logpath = myPipe.data_root+'/'+targ_name+'/log_'+conf_name+'/'
    job_id = job['job_id']
    event_id = job['event_id']
    task_id = job['task_id']
    task = Task.get(task_id)
    task_name = task['name']
    logfile = task_name+'_j'+str(job_id)+'_e'+str(event_id)+'.log'
    try:
     log = open(logpath+logfile, "a")
    except:
     log = open(logpath+logfile, "w")
    log.write(log_text)
    log.close()

if __name__ == "__main__":
    path_to_store = '~/workspace/WINGS/wings_pipe/h5data/wpipe_store.h5'
    # path_to_store='/Users/rubab/Work/WINGS/wings_pipe/h5data/wpipe_store.h5'

    myJob = Job.get(job_id)
    myPipe = Pipeline.get(int(myJob.pipeline_id))
    swroot = myPipe.software_root
    executable = swroot + '/' + task['name']
    dataroot = myPipe.data_root

    catalogID = Options.get('event', event_id)['dp_id']
    catalogDP = DataProduct.get(int(catalogID))
    myTarget = Target.get(int(catalogDP.target_id))
    myConfig = Configuration.get(int(catalogDP.config_id))
    myParams = Parameters.getParam(int(myConfig.config_id))