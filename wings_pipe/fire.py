#! /usr/bin/env python
import argparse,os,subprocess
from wpipe import *

def fire(event):
    name = Event(event,getname)
    value = Event(event,getvalue)
    parent_job = Job.get(int(event.job_id))
    configuration = parent_job.configuration
    pipeline = parent_job.pipeline
    alltasks = Pipeline.get(tasks) # grab all registered task objects
    for i in range(len(alltasks)):
        task = alltasks[i]
        task_id = task.getid
        task_name = task.name
        masks = task.masks #get all masks associated with task
        for j in range(masks):
            mask = masks[j]
            mask_name = mask.name
            mask_value = maks.value
        if (name == mask_name) & (value == mask_value):
            newjob = Job(task=task,config=configuration).create() #need to give this a configuration
            job_id = int(newjob.job_id)
            event_id = int(event.event_id)
            Submit(task,"-e",event_id,"-j",job_id) #pipeline should be able to run stuff and keep track if it completes
