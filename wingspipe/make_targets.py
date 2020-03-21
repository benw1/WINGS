#! /usr/bin/env python
import glob
import json
import os
import shutil

import numpy as np
import wpipe as wp


def register(task):
    _temp = task.mask(source='*', name='start', value=task.name)


def discover_targets(pipeline, data_dir, config_file):
    my_task = pipeline.task(name=__file__.split('/')[-1])
    # configuration stuff
    _params = json.load(open(config_file))[0]
    conffilename = config_file.split('/')[-1]
    confname = conffilename.split('.')[0]
    #
    targlist, ras, decs = get_targ_list(data_dir)
    for index, targ in enumerate(targlist):
        my_target = pipeline.target(name=targ)
        # print("NAME",my_target.name)
        if float(ras[index]) > 0.0:
            print("GOT RA")
            _params['racent'] = float(ras[index])
            _params['deccent'] = float(decs[index])
        conf = my_target.configuration(name=confname, parameters=_params)
        my_job = my_task.job(conf)
        my_job.logprint(''.join(['Target ', targ, ' with ID ', str(my_target.target_id),
                                 ' created with Configuration ', confname, ' and ID ',
                                 str(conf.config_id), " and RA ", str(_params['racent'])]))
        shutil.copy2(config_file, conf.confpath)
        _dp = conf.dataproduct(filename=conffilename, relativepath=pipeline.config_root, group='conf')
        targetfiles = get_target_files(data_dir, targ)
        comp_name = 'completed_' + targ
        options = {comp_name: 0}
        my_job.options = options
        for targetfile in targetfiles:
            shutil.copy2(targetfile, conf.rawpath)
            filename = targetfile.split('/')[-1]
            _dp = conf.dataproduct(filename=filename, relativepath=conf.rawpath, group='raw')
            send(_dp, conf, comp_name, len(targetfiles), my_job)  # send catalog to next step


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
        event = job.child_event('new_stips_catalog', '0', '0',
                                options={'dp_id': dpid, 'to_run': total, 'name': comp_name})
        event.fire()
    elif 'ra' in str(data[0]):
        print('File ', filepath, ' has ra keyword, assuming positions defined')
        event = job.child_event('new_fixed_catalog', '0', '0',
                                options={'dp_id': dpid, 'to_run': total, 'name': comp_name})
        event.fire()

    else:
        print('File ', filepath, ' does not have type keyword, assuming MATCH output')
        event = job.child_event('new_match_catalog', '0', '0',
                                options={'dp_id': dpid, 'to_run': total, 'name': comp_name, 'config_id': confid})
        event.fire()


def parse_all():
    parser = wp.PARSER
    parser.add_argument('--N', '-n', type=str, dest='task_name',
                        help='Name of Task to be Registered')
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
    if args.data_dir is None:
        exit("Need to define a directory with input star lists")
    if args.config_file is None:
        exit("Need to define a configuration file")
    discover_targets(wp.SQLPipeline(int(args.pipeline)), args.data_dir, args.config_file)
    # placeholder for additional steps
    print('done')
