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
