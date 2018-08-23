#! /usr/bin/env python
import argparse,os,subprocess
from wpipe import *

def fire(event):
    name = event.name
    value = event.value
    parent_job = Job.get(int(event.job_id))
    conf_id = parent_job.configuration_id
    configuration = Configuration.get(conf_id)
    pipeline = parent_job.pipeline_id
    allmasks =  Store().select('masks',where="pipeline_id=="+str(pipeline_id))
# grab all registered task objects
    for i in range(allmasks.shape[0]):
        mask = allmasks.iloc[i]
        print(name,mask.name,value,mask.value)
        if (name == mask.name) & (value == mask.value):
            task_id = mask.task_id
            task = Task.get(task_id)
            taskname = task.name
            newjob = Job(task=task,config=configuration).create() #need to give this a configuration
            job_id = int(newjob.job_id)
            event_id = int(event.event_id)
            print(taskname,,"-e",event_id,"-j",job_id)
            #Submit(task,"-e",event_id,"-j",job_id) #pipeline should be able to run stuff and keep track if it completes
