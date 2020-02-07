#! /usr/bin/env python
from wpipe import *
from os import walk
import argparse

def createPipeline(user_name,pipe_tasks_path,decription=''):
    myUser = Store().select('users','name=$user_name')
    pipename = os.path.dirname('.')
    pipeline_root = os.path.realpath('.')
    software_root =  str(pipeline_root)+"/build"
    data_root = str(pipeline_root)+"/data"
    config_root = str(pipeline_root)+"/config"
    myPipe = Pipeline(myUser,pipename,software_root,data_root,pipeline_root,config_root,description).create()
    #for (dirpath,dirnames,filenames) in walk(pipe_tasks_path):
    for task_name in file_list:
        subprocess.call(['cp',pipe_tasks_path+'/'+task_name,data_root+'/.'])
        subprocess.call([data_root+'/'+task_name,'-R'])


def parse_all():
    parser = argparse.ArgumentParser()
    parser.add_argument('--USER', '-u', dest='user_name', help='Name of user creating this pipeline')
    parser.add_argument('--TASKS_PATH', '-c', dest='pipe_tasks_path', help='Path to pipeline tasks to be registered')
    parser.add_argument('--DESCRIP', '-d', dest='decription', default='', help='Optional description of this pipeline')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_all()
    createPipeline(args.user_name,args.pipe_tasks_path,args.description)
