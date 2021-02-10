#! /usr/bin/env python
import time
import wpipe as wp
import fileinput as fi

WAITING_TIME = 0  # 3600*24


def register(task):
    _temp = task.mask(source='*', name='start', value=task.name)
    _temp = task.mask(source='*', name='add_prefix', value='*')


if __name__ == '__main__':
    my_pipe = wp.Pipeline()
    my_job = wp.Job()
    my_job.logprint("Starting add_prefix task")
    my_conf = my_job.config
    filename = my_conf.target.name
    my_conf.target.options['Interactions'] += 1
    my_dp = my_conf.dataproduct(filename, group='raw').make_copy(my_conf.procpath, group='proc')
    my_job.logprint("Copy of raw dataproduct made")
    my_job.logprint("NOW STARTING TO WAIT %f hours" % (WAITING_TIME/3600))
    time.sleep(WAITING_TIME)
    my_job.logprint("NOW ENDING WAIT")
    with fi.input(my_dp.path, inplace=True) as file:
        for line in file:
            print(my_conf.parameters['suffix'] + line, end='')
    my_job.logprint("Edited file to add prefix to it")
    my_job.child_event('add_proc').fire()  # , options={'submission_type': 'pbs'}).fire()
