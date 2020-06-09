#! /usr/bin/env python
import wpipe as wp
import fileinput as fi


def register(task):
    _temp = task.mask(source='*', name='start', value=task.name)
    _temp = task.mask(source='*', name='add_proc', value='*')


if __name__ == '__main__':
    my_pipe = wp.Pipeline()
    my_job = wp.Job()
    my_job.logprint("Starting add_proc task")
    my_conf = my_job.config
    my_dp = my_conf.procdataproducts[0]
    with fi.input(my_dp.path, inplace=True) as file:
        for line in file:
            print(line + " " + my_conf.parameters['proc_word'], end='')
    my_job.logprint("Edited file to make it proc")
    opt_complete = my_conf.target.option("Configuration '"+my_conf.name+"' completed")
    opt_complete.value = True
    my_job.actualize_endtime()
