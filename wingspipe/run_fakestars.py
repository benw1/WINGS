#!/usr/bin/env python

"""
Run Fakestars Task Description:
-------------------
This script is a component of a data processing pipeline designed for Flexible Image Transport System (FITS) files. It carries out several key tasks:

1. Retrieves the target and configuration associated with the job. The target is retrieved from the options of the firing event, and the configuration is used to set up the processing and logging paths.

2. Retrieves the data product for the fake star list from the event.

3. Retrieves the parameter file from the configuration dataproducts.

4. Generates a new parameter file with the fakestar parameters populated, including the name of the fakestar list.

5. Generates the necessary symbolic links to run an independent fake star run with the same data as was used for the original photometry run by run_dolphot.

6. Constructs and executes the Dolphot command using the parameters from the parameter file. The output of the Dolphot operation is logged to a file.

7. The DOLPHOT output from the fake stars is given a data product and counted to see if it is the last one in the group. If it is the last one, as event is fired. 

This script relies on the 'wpipe' library, a Python package designed for efficient pipeline management and execution.
"""

# Original script by Shellby Albrecht
# Modified by Myles McKay
import wpipe as wp
import numpy as np
import os
import time
import sys
import subprocess
from glob import glob
# from astropy.io import fits


def register(task):
    _temp = task.mask(source="*", name="start", value=task.name)
    _temp = task.mask(source="*", name="new_fakestars", value="*")

import signal
def handler(signum, frame):
    print("Forever is over!")
    raise ValueError("end of time")

if __name__ == "__main__":
    my_pipe = wp.Pipeline()
    my_job = wp.Job()
    my_config = my_job.config
    my_target = my_job.target
    this_event = my_job.firing_event
    parent_job = this_event.parent_job
    run_number = this_event.options["run_number"]
    to_run = this_event.options["to_run"]
    this_dp_id = this_event.options["dp_id"]
    this_dp = wp.DataProduct(int(this_dp_id))
    fakelist = this_dp.filename
    my_job.logprint(run_number)
    my_job.logprint(to_run)
    my_job.logprint(this_event.options)
    my_config = my_job.config
    logpath = my_config.logpath
    procpath = my_config.procpath

    # Get parameter file
    param_dp_list = wp.DataProduct.select(config_id=my_config.config_id, subtype="dolphot_parameters")
    param_dp = param_dp_list[0]
    my_job.logprint(f'{param_dp.filename} is the original parameter file')

    param_path = param_dp.relativepath
    param_filename = param_dp.filename
    paramfile = param_path + "/" + param_filename
    paramcontents = np.loadtxt(paramfile,dtype='str',delimiter=",")
    #make new parameter file with the fake star parameters set
    newsuf = "fakepar"+str(run_number)
    fake_param_filename = param_filename.replace("param",newsuf)
    print("name after ",fake_param_filename)
    fake_param = paramfile.replace("param",newsuf)
    alltest = 1 
    if (alltest == 1): 
        with open(fake_param, 'w') as f:
            for line in paramcontents:
                if "xytfile" in line:
                   continue
                if "MaxThreads" in line:
                   f.write("MaxThreads=1\n")
                   continue
                f.write(line+"\n")
            f.write("FakeStars="+fakelist+"\n")    
            f.write("FakeMatch=2\n")    
            f.write("FakePSF=1.5\n")
    if (alltest == 0):
        tagged_dps = []
        ref_dp = wp.DataProduct.select(
            config_id=my_config.config_id, subtype="reference_prepped")
        ref_filt = my_config.parameters["reference_filter"]
        tagged_dps = wp.DataProduct.select(
            config_id=my_config.config_id,
            data_type="image",
            subtype="SCIENCE_prepped")
        my_job.logprint("warm is 1, so removing LW and WFC3/IR from this run")
        my_job.logprint(f"list starts with a length of {len(tagged_dps)}")
        count = 0
        tagged_dps1 = tagged_dps
        tagged_dps = []
        for dp in tagged_dps1:
            if "LONG" in dp.options['channel']:
                continue
            if "IR" in dp.options['detector'] and "CAM" not in dp.options['detector']:
                continue
            tagged_dps.append(dp)
        my_job.logprint(f"list ends with a length of {len(tagged_dps)}")
        my_job.logprint(f"###### This Target: {my_target}\n")

        # Get list of all dataproducts associated with target
        # my_job.logprint(f"Target DPs: {my_target.dataproducts}") #? Not working lisying target dataproducts

        # ref_dp = wp.DataProduct.select(dpowner_id=my_config.config_id, data_type="image")
        #                                subtype="dolphot input reference")  # reference image
        count = 0
        for cand_ref in ref_dp:
            if cand_ref.options["filter"] == ref_filt:
                ref_dp_list = [ref_dp[count]]
            count += 1
        my_job.logprint(f"Reference DP: {ref_dp_list[0].filename}, {type(ref_dp_list[0])}")


        all_dps = ref_dp_list + tagged_dps
        my_job.logprint(f"all_dps: {all_dps}")


        with open(fake_param, 'w') as p:  # create empty file
            nimg = len(all_dps)-1  # number of images
            p.write(f'Nimg={nimg}\n')  # write to file

        # Define image specific parameters
            my_job.logprint(
                f'Checking for user specified individual parameters and defining any unspecified individual parameters')
            count = 0
            for dp in all_dps:  # all images
                my_job.logprint(f'Checking {dp.filename}, {dp}')
                # image number with reference at index 0
                # loc = tagged_dps.index(dp)
                loc = all_dps.index(dp)
    
                im_fullfile = dp.filename
                im_file = im_fullfile.split('.fits')[0]  # get rid of extension
                p.write(f'img{loc}_file = {im_file}\n')
                if "JWST" in dp.options['telescope']:
                    im_pars = ["apsky", "shift", "xform",
                               "raper", "rchi", "rsky0", "rsky1", "rsky2", "rpsf"]
                    def_vals = ["20 35", "0 0", "1 0 0", "2", "1.5", "15", "35", "3 10",  "15"]
                else:
                    im_pars = ["apsky", "shift", "xform",
                               "raper", "rchi", "rsky0", "rsky1", "rpsf"]
                    def_vals = ["20 35", "0 0", "1 0 0", "2", "1.5", "15", "35",  "15"]
                if 'reference' not in dp.subtype:
                    defined = []
                    count += 1
                    img = 'img'+str(count)
                    parcount = 0
                    for impar in im_pars:
                        param_name = "img"+str(count)+"_"+impar
                        cam_name = dp.options['detector']+"_"+impar
                        if "NIRCAM" in dp.options['detector']:
                            if "LONG" in dp.options['channel']:
                                cam_name = dp.options['detector']+"LW_"+impar
                        try:
                            p.write(
                                f'{param_name} = {my_config.parameters[cam_name]}\n')
                            my_job.logprint(
                                f'{param_name} parameter found in configuration')
                            defined.append(1)
                        except:
                            p.write(
                                f'{param_name} = {def_vals[parcount]}\n')
                            my_job.logprint(
                                f'{param_name} parameter default')
                            defined.append(1)
                        parcount += 1
    
    # Define global parameters
            my_job.logprint(
                f'Checking for user specified global parameters and defining any unspecified global parameters')
            params_global = ["UseWCS", "PSFPhot", "FitSky", "SkipSky", "SkySig", "SecondPass", "SearchMode", "SigFind", "SigFindMult", "SigFinal", "MaxIT", "NoiseMult", "FSat", "FlagMask", "ApCor", "Force1", "Align", "aligntol",
                             "alignstep", "ACSuseCTE", "WFC3useCTE", "Rotate", "RCentroid", "PosStep", "dPosMax", "RCombine", "SigPSF", "PSFres", "psfoff", "DiagPlotType", "CombineChi", "ACSpsfType", "WFC3IRpsfType", "WFC3UVISpsfType", "PSFPhotIt"]
            glob_vals = ["2", "1", "2", "2", "2.25", "5", "1", "3.0", "0.85", "3.5", "25", "0.10", "0.999", "4", "1", "1",
                         "2", "4", "2", "0", "0", "1", "1", "0.1", "2.5", "1.415", "3.0", "1", "0.0", "PNG", "1", "0", "0", "0", "2"]
            # params_global = ["MaxIT","PSFPhot", "PSFPhotIt", "FitSky", "SkipSky", "SkySig", "SigFindMult", "FSat", "PosStep", "sigPSF", "UseWCS", "NoiseMult", "SecondPass", "Force1", "WFC3UVISpsfType","ACSpsfType","WFC3IRpsfType","ACSuseCTE", "WFC3useCTE","FlagMask","InterpPSFlib", "CombineChi", "RCombine", "PSFres"]
            # glob_vals = ["25","1","2","2","1","2.25","0.85","0.999","0.25","5.0","2","0.1","5","0","0","0","0","0","0","4","1","0","1.5","1"]
            paramcount = 0
            for globpar in params_global:
                try:
                    my_config.parameters[globpar]
                    p.write(f'{globpar} = {my_config.parameters[globpar]}\n')
                    my_job.logprint(f'{globpar} parameter found in configuration')
                except:
                    p.write(f'{globpar} = {glob_vals[paramcount]}\n')
                    my_job.logprint(f'{globpar} parameter set to default')
                paramcount += 1
            p.write("FakeStars="+fakelist+"\n")    
            p.write("FakeMatch=2\n")    
            p.write("FakePSF=1.5\n")
    
    my_job.logprint(f'{fake_param} is the fakestar parameter file')  
    wp.DataProduct(my_config, filename=fake_param_filename, group="conf", data_type="fakepars", subtype="fake_param")
    #grab all the dolphot data products, to use for making links with run number
    dolphot_dps = wp.DataProduct.select(config_id=my_config.config_id, subtype="dolphot output")
    if len(dolphot_dps) < 5:
        raise exception("only ",{len(dolphot_dps)}," dolphot products found")

    #dolphotout = procpath + "/" + my_target.name + ".phot" + "_" + str(run_number)
    dolphotout = procpath + "/" + fake_param_filename.split('.')[0] + ".phot" + "_" + str(run_number)
    logdp = my_job.logprint()
    logfile = logpath + "/" + logdp.filename
    newfakefile = dolphotout+".fake"
    if os.path.isfile(newfakefile):
        print("out is here")
    else:
        for dp in dolphot_dps:
            if "prewarm" in dp.filename:
                continue
            new_name = dp.filename.replace("phot", "phot_"+str(run_number))
            link_command = "ln -s "+dp.filename+" "+procpath+"/"+new_name
            my_job.logprint(f'making link: {link_command}')
            os.system(link_command) 
        
    # # Run Dolphot
    need = 0
    if os.path.isfile(newfakefile):
        try:
            with open(newfakefile, 'r', encoding='utf-8', errors='ignore') as f:
                for countl, _ in enumerate(f, 1):
                    if countl > 100:
                        need =1
                        my_job.logprint(f"DOLPHOT already done for {fake_param} and {dolphotout}, continuing...")
        except:
            need=0
    if need == 0:
        my_job.logprint(f"Running DOLPHOT on {fake_param} and {dolphotout}")
        dolphot_command = "cd "+procpath+" && " + \
            my_config.parameters["dolphot_path"]+"dolphot " + dolphotout + \
            ' -p' + param_path + "/" +  fake_param_filename + " >> "+logfile
        my_job.logprint(dolphot_command)
        dolphot_output = os.system(dolphot_command)
    # check that this gets file called just dolphotout
    phot_dp = wp.DataProduct(my_config, filename=dolphotout+".fake", group="proc", subtype="fake_output")
    
    for dp in dolphot_dps:
        if "prewarm" in dp.filename:
            continue
        new_name = dp.filename.replace("phot", "phot_"+str(run_number))
        rm_command = "rm "+" "+procpath+"/"+new_name
        my_job.logprint(f'removing link: {rm_command}')
        os.system(rm_command) 
        

    my_job.logprint(
        f"Created dataproduct for {dolphotout}.fake, {phot_dp}")
    compname = this_event.options["compname"]
    comp_jobid = int(this_event.options["comp_jobid"])
    compjob = wp.Job(comp_jobid)
    update_option = compjob.options[compname]
    update_option += 1
    to_run = this_event.options["to_run"]
    my_job.logprint(f"comp_job options: {compjob.options}")
    my_job.logprint(f"{update_option}/{to_run} TAGGED")

    if update_option == to_run:
        next_event = my_job.child_event(
          name="fakestars_done",
          options={"config_id": my_config.config_id, "memory": "150G",'submission_type': 'scheduler'}
        )  # next event
        next_event.fire()
        time.sleep(150)

    
