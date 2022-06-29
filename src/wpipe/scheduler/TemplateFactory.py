#!/usr/bin/env python
"""
Contains the TemplateFactory class definition.

Please note that this module is private and should only be used internally to
the schedulers subpackage.
"""
from jinja2 import Template


class TemplateFactory(object):

    @staticmethod
    def getJobListTemplate():
        templateString = \
            """{% for job in jobs -%}
    {{ job.command }}
{% endfor %}
"""

        return Template(templateString)

    @staticmethod
    def getPbsFileTemplate():
        templateString = \
            """#PBS -S /bin/bash
#PBS -j oe
#PBS -l select={{pbs.nnodes}}:ncpus={{pbs.ncpus}}:{{pbs.ompthreads}}model={{pbs.model}}
#PBS -W group_list=s1692
#PBS -l walltime={{pbs.walltime}}
source ~/.bashrc
cd {{pbs.pipe_root}}
parallel --jobs {{pbs.njobs}} --sshloginfile $PBS_NODEFILE --workdir $PWD < {{pbs.executables_list_path}}

"""

        return Template(templateString)
        
    @staticmethod
    def getSlurmFileTemplate():
        templateString = \
            """#!/bin/bash
## Job Name
#SBATCH --job-name={{slurm.jobid}}
## Allocation Definition 
#SBATCH --account={{slurm.account}}
#SBATCH --partition={{slurm.partition}}
## Resources
## Nodes
#SBATCH --nodes={{slurm.nnodes}} --ntasks-per-node={{slurm.njobs}} 
## Walltime (4 hours)
#SBATCH --time={{slurm.walltime}}
## Memory per node
#SBATCH --mem={{slurm.mem}}
## Specify the working directory for this job
#SBATCH --chdir={{slurm.pipe_root}}
module load parallel-20170722 
cat {{slurm.executables_list_path}} | parallel

"""

        return Template(templateString)
