#!/usr/bin/env python

"""
Split Fakestars Task Description:
-------------------
This script is a component of a data processing pipeline designed for Flexible Image Transport System (FITS) files. It carries out several key tasks:

1. Using the configuration ID in the parent event, it searches the configuration. 

2.Looks for files in the configuration proc/ subdirectory for a file ending in .fakelist. If no such file is found, it reports that to the log and exits.  If it is found, it creates a data product, divides the file into subfiles each containing 50 lines.

3. It creates data products for each of the newly-generated list files

4. It counts the total number of the list files and fires an event for each one, with an option that will result in a job being sent to the ckpt queue.

This script relies on the 'wpipe' library, a Python package designed for efficient pipeline management and execution.
"""

import wpipe as wp
import numpy as np
import math
import time
import glob
import os

# Task will look at all drizzled images for a target and choose the best
#  reference to use for DOLPHOT


def register(task):
    _temp = task.mask(source="*", name="start", value=task.name)
    _temp = task.mask(source="*", name="fakelist_ready", value="*")

def split_file(this_event, my_config, my_job, my_target, starsper):
    # dataproducts for the drizzled images for my_target
    this_dp_id = this_event.options["phot_dp_id"]
    phot_dp = wp.DataProduct(int(this_dp_id), group="proc")

    conf_fs = f"{my_config.procpath}/{my_target.name}.fakelist"
    my_job.logprint(f"Checking {conf_fs}")
    fs_list = glob.glob(conf_fs)
    if (len(fs_list) == 0):
        my_job.logprint("No Fakestars Found")
        raise ValueError('No Fakestars Found')
    head_tail = os.path.split(fs_list[0])
    my_fsdp = my_config.dataproduct(
        filename=head_tail[1], group="proc", data_type="raw_fakelist"
        )
    # my_dps = wp.DataProduct.select(wp.si.DataProduct.filename.regexp_match("final*"), dpowner_id=my_job.config_id)
    # my_job.logprint(f"{my_dps}")

    my_job.logprint(
        f"{fs_list[0]} found for {my_target.name}, {my_config.name}.")

# divding up the fake stars and making dataproducts
    fullpath = fs_list[0]
    fsarr = np.loadtxt(fullpath, dtype='str')
    totfiles = int(math.ceil(len(fsarr)/starsper))
    comp_name = "completed_" + my_target.name
    new_option = {comp_name: 0}
    my_job.options = new_option
    totruns = 0
    photfilename = phot_dp.filename
    for i in np.arange(totfiles):
        #filename = "fake_"+str(i+1)+".lst"
        filename = photfilename+"_"+str(i+1)+".fake"
        filepath = my_config.procpath + "/" + filename
        if os.path.isfile(filepath):
           my_job.logprint(f"skipping {filepath} as already done...")
           continue
        else:
           if totruns == 0:
              runmin = int(math.floor(i/1000.0))
           totruns += 1
    for i in np.arange(totfiles):
        filename = photfilename+"_"+str(i+1)+".fake"
        filepath = my_config.procpath + "/" + filename
        if os.path.isfile(filepath):
           continue
        else:
           filename = "fake_"+str(i+1)+".lst"
           filepath = my_config.procpath + "/" + filename
           minstar = int(i*starsper)
           maxstar = minstar+int(starsper)
           if maxstar > len(fsarr):
              maxstar = len(fsarr)
           newarr = fsarr[minstar:maxstar,:]
           np.savetxt(filepath, newarr, fmt="%s")
    all_split = int(math.ceil(totfiles/(1000))) 
    my_job.logprint(''.join(["runmin and all_split are ",str(runmin),str(all_split)]))
    for j in np.arange(start=runmin,stop=all_split): 
        minlist=int(j*1000)+1
        maxlist=int(minlist+1000)
        if maxlist > totfiles+1:
           maxlist = totfiles+1
        my_event = my_job.child_event(
           name="fakelist_ready", tag=minlist,
           options={
               'min': minlist,
               'max': maxlist,
               'submission_type': 'scheduler',
               'phot_dp_id' : this_dp_id,
               'target_id': this_event.options['target_id'],
               'to_run': totruns,
               'compname': comp_name,
               'comp_jobid' : my_job.job_id,
               'config_id': my_config.config_id,
               'walltime': "6:00:00"
           }
        )
        my_event.fire()
        time.sleep(0.5)
    return 1

def send(this_event,my_config,my_job):
    this_dp_id = this_event.options["phot_dp_id"]
    phot_dp = wp.DataProduct(int(this_dp_id), group="proc")
    photfilename = phot_dp.filename
    minlist = int(this_event.options['min'])
    maxlist = int(this_event.options['max'])
    totfiles = maxlist-minlist
    my_job.logprint(''.join(["Maxlist and minlist are ",str(maxlist)," and ",str(minlist)]))
    for i in np.arange(totfiles):
        filename = photfilename+"_"+str(i+minlist)+".fake"
        filepath = my_config.procpath + "/" + filename
        if os.path.isfile(filepath):
            my_job.logprint(f"{filename} already exists, skipping...")
            continue
        else:
            filename = "fake_"+str(i+minlist)+".lst"
            filepath = my_config.procpath + "/" + filename
            my_sub = my_config.dataproduct(
                filename=filename, group="proc", subtype="sub_fakelist"
            )
            # have to define dp_id outside of the event or else it sets it as the same for all dps
            dp_id = my_sub.dp_id
            my_job.logprint(f"Firing new_fakestars event for {filename}")
            my_event = my_job.child_event(
                name="new_fakestars", tag=dp_id,
                options={
                    'dp_id': dp_id,
                    'to_run': this_event.options['to_run'],
                    'compname': this_event.options['compname'],
                    'comp_jobid': this_event.options['comp_jobid'],
                    'submission_type': 'scheduler',
                    'config_id': my_config.config_id,
                    'account': "astro-ckpt",
                    'partition': "ckpt",
                    'walltime': "6:00:00",
                    'run_number': i+minlist,
                    'memory': "300G"
                }
            )
            my_event.fire()
            time.sleep(10)

    return 1

# Setting up the task

if __name__ == "__main__":
    my_pipe = wp.Pipeline()
    my_job = wp.Job()
    starsper = 1000.0
# Defining the target and dataproducts
    this_event = my_job.firing_event  # parent astrodrizzle event firing
    #   my_job.logprint(f"{parent_event}")

    my_target = wp.Target(this_event.options["target_id"])  # get the target

    my_config = my_job.config  # configuration for this job
    #   my_job.logprint(my_config)
    print("NAME ",this_event.name)    
    my_job.logprint(''.join(["NAME ",this_event.name]))

    if "start" in this_event.name:
       my_job.logprint("going to first_run")
       first_run = split_file(this_event,my_config,my_job,my_target,starsper) 
    if "ready" in this_event.name:
       my_job.logprint("going to send")
       count=send(this_event,my_config,my_job)
    time.sleep(130)
