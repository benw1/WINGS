#! /usr/bin/env python
import os
import wpipe as wp


def register(task):
    _temp = task.mask(source='*', name='start', value=task.name)
    _temp = task.mask(source='*', name='__init__', value='*')


if __name__ == '__main__':
    my_pipe = wp.Pipeline()
    my_job = wp.Job()
    my_job.logprint("Preparing targets")
    for my_input in my_pipe.inputs:
        for my_dp in my_input.rawdataproducts:
            my_target = my_input.target(name=my_dp.filename)
            for my_conf in my_target.configurations:
                my_target.options = {"Configuration '"+my_conf.name+"' completed": False}
                my_job.logprint("Starting target "+my_target.name+" config "+my_conf.name)
                if 'WPIPE_NO_PBS_SCHEDULER' in os.environ.keys():
                    my_job.child_event('add_prefix', tag="config_id#"+str(my_conf.config_id),
                                       options={'config_id': my_conf.config_id}).fire()
                else:
                    my_job.child_event('add_prefix', tag="config_id#"+str(my_conf.config_id),
                                       options={'config_id': my_conf.config_id, 'submission_type': 'pbs', 'job_time': 5}).fire()
                    my_job.child_event('add_prefix', tag="config_id2_#" + str(my_conf.config_id),
                                       options={'config_id': my_conf.config_id, 'submission_type': 'pbs', 'job_time': 5}).fire()

