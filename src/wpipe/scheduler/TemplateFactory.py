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
#PBS -l select=1:ncpus=10:model=has
#PBS -W group_list=s1692
#PBS -l walltime=24:00:00
source ~/.bashrc
cd {{pbs.pipe_root}}
parallel --jobs {{pbs.njobs}} --sshloginfile $PBS_NODEFILE --workdir $PWD < {{pbs.executables_list_path}}

"""

        return Template(templateString)