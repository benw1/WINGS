from .core import *
from .User import User
from .Node import Node
from .Pipeline import Pipeline
from .Input import Input
from .Option import Option
from .Target import Target
from .Configuration import Configuration
from .Parameter import Parameter
from .DataProduct import DataProduct
from .Task import Task
from .Mask import Mask
from .Job import Job
from .Event import Event


DefaultUser = User()
DefaultNode = Node()


def sql_hyak(task, job_id, event_id):
    my_job = Job(job_id)
    my_pipe = my_job.pipeline
    swroot = my_pipe.software_root
    executable = swroot + '/' + task.name
    catalog_id = Event(event_id).options['dp_id']
    catalog_dp = DataProduct(catalog_id)
    my_config = catalog_dp.config
    slurmfile = my_config.confpath + '/' + task.name + '_' + str(job_id) + '.slurm'
    # print(event_id,job_id,executable,type(executable))
    eidstr = str(event_id)
    jidstr = str(job_id)
    print("Submitting ", slurmfile)
    with open(slurmfile, 'w') as f:
        f.write('#!/bin/bash' + '\n' +
                '## Job Name' + '\n' +
                '#SBATCH --job-name=' + jidstr + '\n' +
                '## Allocation Definition ' + '\n' +
                '#SBATCH --account=astro' + '\n' +
                '#SBATCH --partition=astro' + '\n' +
                '## Resources' + '\n' +
                '## Nodes' + '\n' +
                '#SBATCH --ntasks=1' + '\n' +
                '## Walltime (10 hours)' + '\n' +
                '#SBATCH --time=10:00:00' + '\n' +
                '## Memory per node' + '\n' +
                '#SBATCH --mem=10G' + '\n' +
                '## Specify the working directory for this job' + '\n' +
                '#SBATCH --workdir=' + my_config.procpath + '\n' +
                'source activate forSTIPS3' + '\n' +
                executable + ' -e ' + eidstr + ' -j ' + jidstr)
    subprocess.run(['sbatch', slurmfile], cwd=my_config.confpath)


def sql_pbs(task, job_id, event_id):
    my_job = Job(job_id)
    my_pipe = my_job.pipeline
    swroot = my_pipe.software_root
    executable = swroot + '/' + task.name
    catalog_id = Event(event_id).options['dp_id']
    catalog_dp = DataProduct(catalog_id)
    my_config = catalog_dp.config
    # pbsfile = my_config.confpath + '/' + task.name + '_' + str(job_id) + '.pbs'
    pbsfile = '/home1/bwilli24/Wpipelines/' + task.name + '_jobs'
    # print(event_id,job_id,executable,type(executable))
    eidstr = str(event_id)
    jidstr = str(job_id)
    print("Submitting ", pbsfile)
    # with open(pbsfile, 'w') as f:
    with open(pbsfile, 'a') as f:
        f.write(  # '#PBS -S /bin/csh' + '\n'+
            # '#PBS -j oe' + '\n'+
            # '#PBS -l select=1:ncpus=4:model=san' + '\n'+
            # '#PBS -W group_list=s1692' + '\n'+
            # '#PBS -l walltime=10:00:00' + '\n'+

            # 'cd ' + myConfig.procpath  + '\n'+

            # 'source activate STIPS'+'\n'+

            # executable+' -e '+eidstr+' -j '+jidstr)
            'source /nobackupp11/bwilli24/miniconda3/bin/activate STIPS && ' + executable + ' -e ' + eidstr + ' -j ' + jidstr + '\n')
    subprocess.run(['qsub', pbsfile], cwd=my_config.confpath)
