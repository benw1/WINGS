#! /usr/bin/env python
import subprocess
from shutil import which

import wpipe as wp


def register(task):
    _temp = task.mask(source='*', name='start', value='*')
    _temp = task.mask(source='*', name='parameters_written', value='*')


def run_dolphot(dp_id):
    param_dp = wp.DataProduct(dp_id)
    my_target = param_dp.target
    my_config = param_dp.config
    targname = my_target.name
    outfile = targname + '.phot'
    logfile = outfile + ".log"
    dolphot_path = which('wfirstmask')
    dolphot_path = dolphot_path[:-10]
    dolphot = dolphot_path + "dolphot"
    parameter_file = param_dp.relativepath + '/' + param_dp.filename
    subprocess.run([dolphot, outfile, " -p " + parameter_file, " > ", my_config.logpath + '/' + logfile],
                   cwd=my_config.procpath)


def hyak_dolphot(dp_id):
    param_dp = wp.DataProduct(dp_id)
    parameter_file = param_dp.relativepath + '/' + param_dp.filename
    my_target = param_dp.target
    my_config = param_dp.config
    targname = my_target.name
    outfile = targname + '.phot'
    logfile = outfile + ".log"
    dolphot_path = which('wfirstmask')
    dolphot_path = dolphot_path[:-10]
    dolphot = dolphot_path + "dolphot"
    slurmfile = parameter_file + '.slurm'
    with open(slurmfile, 'w') as f:
        f.write('#!/bin/bash' + '\n' +
                '## Job Name' + '\n' +
                '#SBATCH --job-name=dolphot_' + str(dp_id) + '\n' +
                '## Allocation Definition ' + '\n' +
                '#SBATCH --account=astro' + '\n' +
                '#SBATCH --partition=astro' + '\n' +
                '## Resources' + '\n' +
                '## Nodes' + '\n' +
                '#SBATCH --ntasks=1' + '\n' +
                '## Walltime (3 hours)' + '\n' +
                '#SBATCH --time=100:00:00' + '\n' +
                '## Memory per node' + '\n' +
                '#SBATCH --mem=10G' + '\n' +
                '## Specify the working directory for this job' + '\n' +
                '#SBATCH --workdir=' + my_config.procpath + '\n' +
                '##turn on e-mail notification' + '\n' +
                dolphot + " " + outfile + " -p" + parameter_file + " >" + my_config.logpath + '/' + logfile)
    subprocess.run(['sbatch', slurmfile], cwd=my_config.procpath)


# def create_target(a=''):
#     pass


def parse_all():
    parser = wp.PARSER
    parser.add_argument('--E', '-e', type=int, dest='event_id',
                        help='Event ID')
    parser.add_argument('--J', '-j', type=int, dest='job_id',
                        help='Job ID')
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
    # hyak_dolphot(this_dp_id)
    run_dolphot(this_dp_id)
