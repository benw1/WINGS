from .core import *
from .User import SQLUser
from .Node import SQLNode
from .Pipeline import SQLPipeline
from .Option import SQLOption
from .Target import SQLTarget
from .Configuration import SQLConfiguration
from .Parameter import SQLParameter
from .DataProduct import SQLDataProduct
from .Task import SQLTask
from .Mask import SQLMask
from .Job import SQLJob
from .Event import SQLEvent


DefaultUser = SQLUser()
DefaultNode = SQLNode()


def sql_submit(task, job_id, event_id):
    my_pipe = task.pipeline
    executable = my_pipe.software_root + '/' + task.name
    # subprocess.Popen([executable,'-e',str(event_id),'-j',str(job_id)],cwd=my_pipe.data_root) # This line will work with an SQL backbone, but NOT hdf5, as 2 tasks running on the same hdf5 file will collide!
    subprocess.run([executable, '-e', str(event_id), '-j', str(job_id)], cwd=my_pipe.data_root)
    # Let's send stuff to slurm
    # sql_hyak(task,job_id,event_id)
    # Let's send stuff to pbs
    # sql_pbs(task,job_id,event_id)
    return


def sql_hyak(task, job_id, event_id):
    my_job = SQLJob(job_id)
    my_pipe = my_job.pipeline
    swroot = my_pipe.software_root
    executable = swroot + '/' + task.name
    catalog_id = SQLEvent(event_id).options['dp_id']
    catalog_dp = SQLDataProduct(catalog_id)
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
    my_job = SQLJob(job_id)
    my_pipe = my_job.pipeline
    swroot = my_pipe.software_root
    executable = swroot + '/' + task.name
    catalog_id = SQLEvent(event_id).options['dp_id']
    catalog_dp = SQLDataProduct(catalog_id)
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


def sql_fire(event):
    event_name = event.name
    event_value = event.value
    event_id = event.event_id
    # print("HERE ",event.name," DONE")
    parent_job = event.parent_job
    try:
        configuration = SQLConfiguration(event.options['config_id'])
    except:
        configuration = parent_job.config
    # print(parent_job.pipeline_id)
    for task in parent_job.pipeline.tasks:
        # print(task.task_id)
        for mask in task.masks:
            mask_name = mask.name
            mask_value = mask.value
            # print("HERE",event_name,mask_name,event_value,mask_value,"DONE3")
            if (event_name == mask_name) & ((event_value == mask_value) | (mask_value == '*')):
                taskname = task.name
                newjob = event.fired_job(task, configuration)
                job_id = newjob.job_id
                print(taskname, "-e", event_id, "-j", job_id)
                sql_submit(task, job_id, event_id)  # pipeline should be able to run stuff and keep track if it completes
                return


def sql_logprint(configuration, job, log_text):
    logpath = configuration.target.relativepath + '/log_' + configuration.name + '/'
    logfile = job.task.name + '_j' + str(job.job_id) + '_e' + str(job.firing_event_id) + '.log'
    try:
        log = open(logpath + logfile, "a")
    except:
        log = open(logpath + logfile, "w")
    log.write(log_text)
    log.close()
