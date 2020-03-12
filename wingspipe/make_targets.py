#! /usr/bin/env python
import argparse
import glob
import json
import os
import subprocess

import numpy as np
import wpipe as wp


def register(task):
    _temp = task.mask(source='*', name='start', value=task.name)


def discover_targets(pipeline, config_file, data_dir):
    my_pipe = pipeline
    my_target = my_pipe.target(name='start_targ')
    targlist, ras, decs = get_targ_list(data_dir)
    my_config = my_target.configuration(name='test_config',
                                        parameters={'a': 0,
                                                    'x': 12,
                                                    'note': 'testing this'})
    my_job = my_config.job()  # need to create dummy job to keep track of events
    count = -1
    for targ in targlist:
        count += 1
        my_target = my_pipe.target(name=targ)
        # print("NAME",my_target.name)
        _params = json.load(open(config_file))[0]
        if float(ras[count]) > 0.0:
            print("GOT RA")
            _params['racent'] = float(ras[count])
            _params['deccent'] = float(decs[count])
        conffilename = config_file.split('/')[-1]
        confname = conffilename.split('.')[0]
        conf = my_target.configuration(name=confname, parameters=_params)
        wp.sql_logprint(conf, my_job, ''.join(
            ['Target ', targ, ' with ID ', str(my_target.target_id), ' created with Configuration ', confname, ' and ID ',
             str(conf.config_id), " and RA ", str(_params['racent'])]))
        _temp = subprocess.run(['cp', config_file, conf.confpath + '/'], stdout=subprocess.PIPE)
        _dp = conf.dataproduct(filename=conffilename, relativepath=my_pipe.config_root, group='conf')
        targetfiles = get_target_files(data_dir, targ)
        comp_name = 'completed' + targ
        options = {comp_name: 0}
        _opt = my_job.options = options
        for files in targetfiles:
            subprocess.run(['cp', files, conf.rawpath], stdout=subprocess.PIPE)
            filename = files.split('/')[-1]
            _dp = conf.dataproduct(filename=filename, relativepath=conf.rawpath.values[0], group='raw')
            send(_dp, conf, comp_name, len(targetfiles), my_job)  # send catalog to next step
    return


def get_targ_list(data_dir):
    if os.path.exists(data_dir + "/targets"):
        data, ras, decs = np.loadtxt(data_dir + "/targets", dtype=str).T
    else:
        data = os.listdir(data_dir)
        shape = np.shape(data)
        ras = np.zeros(shape)
        decs = np.zeros(shape)
    target_names = []
    for dat in data:
        # each prefix is a target
        checkname = dat.split('.')[:-1][0]
        if checkname in target_names:
            continue
        target_names.append(dat.split('.')[:-1][0])
    return target_names, ras, decs


def get_target_files(data_dir, targ):
    targfiles = glob.glob(data_dir + '/' + targ + '*')
    return targfiles


def send(dp, conf, comp_name, total, job):
    filepath = dp.relativepath + '/' + dp.filename
    dpid = int(dp.dp_id)
    confid = int(conf.config_id)
    print('TEST', dp.filename, filepath)
    data = np.loadtxt(filepath, dtype=str, usecols=0)
    if 'type' in str(data[0]):
        print('File ', filepath, ' has type keyword, assuming STIPS-ready')
        event = job.event('new_stips_catalog', '0', '0', options={'dp_id': dpid, 'to_run': total, 'name': comp_name})
        wp.fire(event)
    elif 'ra' in str(data[0]):
        print('File ', filepath, ' has ra keyword, assuming positions defined')
        event = job.event('new_fixed_catalog', '0', '0', options={'dp_id': dpid, 'to_run': total, 'name': comp_name})
        wp.fire(event)

    else:
        print('File ', filepath, ' does not have type keyword, assuming MATCH output')
        event = job.event('new_match_catalog', '0', '0',
                          options={'dp_id': dpid, 'to_run': total, 'name': comp_name, 'config_id': confid})
        wp.fire(event)
    return


def parse_all():
    parser = wp.PARSER
    parser.add_argument('--R', '-R', dest='REG', action='store_true',
                        help='Specify to Register')
    parser.add_argument('--N', '-n', type=str, dest='task_name',
                        help='Name of Task to be Registered')
    parser.add_argument('--P', '-p', type=int, dest='PID',
                        help='Pipeline ID')
    parser.add_argument('--C', '-c', type=str, dest='config_file',
                        help='Configuration File Path')
    parser.add_argument('--T', '-t', type=str, dest='data_dir',
                        help='Path to directory with input lists')
    parser.add_argument('--E', '-e', type=int, dest='event_id',
                        help='Event ID')
    parser.add_argument('--J', '-j', type=int, dest='job_id',
                        help='Job ID')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_all()
    if args.REG:
        register(wp.SQLPipeline(int(args.PID)).task(name=str(args.task_name)))
    else:
        if args.PID is None:
            exit("Need to define a pipeline ID")
        if args.config_file is None:
            exit("Need to define a configuration file")
        if args.data_dir is None:
            exit("Need to define a directory with input star lists")
        myPipe = wp.Pipeline.get(args.PID)
        discover_targets(myPipe, args.config_file, args.data_dir)

    # placeholder for additional steps
    print('done')
