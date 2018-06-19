#! /usr/bin/env python
import argparse, os, subprocess
from wpipe import *

def createPipeline(user_name,pipe_tasks_path,description=''):
    myUser   = Store().select('users',''.join(('name=="',str(user_name),'"')))
    pipeRoot = os.getcwd()
    pipeName = os.path.basename(pipeRoot)
    softRoot = pipeRoot+'/build'
    dataRoot = pipeRoot+'/data'
    confRoot = pipeRoot+'/config'
    myPipe   = Pipeline(myUser,pipeName,softRoot,dataRoot,
                pipeRoot,confRoot,description).create()

    myPipe.to_json('pipe.conf',orient='records')
    
    _t = subprocess.call(['mkdir',softRoot,dataRoot,confRoot],stdout=subprocess.PIPE)
    
    taskList = os.listdir(pipe_tasks_path)

    for _task in taskList:
        _t = subprocess.call(['cp',''.join((pipe_tasks_path,'/',_task)),
                              ''.join((softRoot,'/.'))],
                             stdout=subprocess.PIPE)

    for _task in taskList:
        if (('.py' in _task)&(_task!='wpipe.py')):
            _t = subprocess.call([''.join((softRoot,'/',_task)),'-R',
                                  '-p',str(int(myPipe.pipeline_id)),
                                  '-n',str(_task)],
                                  stdout=subprocess.PIPE)
    return None

def parse_all():
    parser = argparse.ArgumentParser()
    parser.add_argument('--USER', '-u', dest='user_name',
                        help='Name of user creating this pipeline')
    parser.add_argument('--TASKS_PATH', '-c', dest='pipe_tasks_path',
                        help='Path to pipeline tasks to be registered')
    parser.add_argument('--DESCRIP', '-d', dest='description', default='',
                        help='Optional description of this pipeline')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_all()
    createPipeline(args.user_name,args.pipe_tasks_path,args.description)
