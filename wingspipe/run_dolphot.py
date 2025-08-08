#! /usr/bin/env python
import subprocess
from shutil import which

import wpipe as wp
import importlib
import os
import glob

def register(task):
    _temp = task.mask(source='*', name='start', value='*')
    _temp = task.mask(source='*', name='parameters_written', value='*')


def run_dolphot(my_job,dp_id):
    param_dp = wp.DataProduct(dp_id)
    my_target = param_dp.target
    my_config = param_dp.config
    targname = my_target.name
    #my_config.parameters['dolphot_path'] = "/gscratch/astro/benw1/dolphot_roman/dolphot3.0/bin/"
    dolphot_path = my_config.parameters['dolphot_path']
    logpath = my_config.logpath
    procpath = my_config.procpath

    my_job.logprint(dolphot_path)
    dolphot = dolphot_path + "/" + "dolphot"
    my_job.logprint(dolphot)
    parameter_file = param_dp.relativepath + '/' + param_dp.filename
    parfilename = param_dp.filename
    detname = parfilename.split('.')[0]
    outfile = detname + '.phot'
    logdp = my_job.logprint()
    logfile = logpath + "/" + logdp.filename
    my_job.logprint(f"Running DOLPHOT on {parameter_file}")
    dolphot_command = "cd " + procpath + " && " + \
        dolphot+" "+ outfile + ' -p' + parameter_file + " >> "+logfile
    my_job.logprint(dolphot_command)
    if os.path.isfile(procpath+"/"+outfile):
        my_job.logprint(f"{procpath}/{outfile} already exists... not running DOLPHOT")
    else:
        my_job.logprint(f"{procpath}/{outfile} does not exist... running DOLPHOT")
        dolphot_output = os.system(dolphot_command)
    try:
        head_tail = os.path.split(outfile)
        phot_dp = wp.DataProduct(
            my_config, filename=head_tail[1], group="proc", subtype="dolphot output")
    except:
        ValueError("Failed to create phot file DP. Exiting.") 
    my_job.logprint(
        f"Created dataproduct for {head_tail[1]}, {phot_dp}")
    out_files = glob.glob(procpath+'/*.phot.*')
    if len(out_files) < 5:
        raise exception("too few output files")
    for file in out_files:
        head_tail = os.path.split(file)
        dolphot_output_dp = wp.DataProduct(
            my_config, filename=head_tail[1], group="proc", subtype="dolphot output")
        my_job.logprint(
            f"Created dataproduct for {file}, {dolphot_output_dp}")

    return phot_dp.dp_id
 

# def create_target(a=''):
#     pass


def parse_all():
    parser = wp.PARSER
    parser.add_argument('--DP', '-dp', type=int, dest='dp_id',
                        help='Dataproduct ID')

    return parser.parse_args()


if __name__ == '__main__':
    args = parse_all()
    this_job_id = args.job_id
    this_job = wp.Job(this_job_id)
    this_event = this_job.firing_event
    this_event_id = this_event.event_id
    this_dp_id = this_event.options['dp_id']
    
    phot_dp_id=run_dolphot(this_job,this_dp_id)
     
    new_event = this_job.child_event('dolphot_done', tag=phot_dp_id, options={'dp_id': phot_dp_id,'submission_type':'scheduler'})
    print("Firing dolphot_done event")
    this_job.logprint(''.join(["Firing event ", str(new_event.event_id), "  dolphot_done"]))
    new_event.fire()
