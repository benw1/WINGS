#! /usr/bin/env python
import time, os, sys, json
import numpy as np
import pandas as pd
from astropy import wcs
from astropy.io import fits, ascii
from astropy.table import Table

def _to_dict(x):
    '''x is any class instance'''
    return x.__dict__

def _to_json(x):
    '''x is any class instance'''
    return json.dumps(x.__dict__)

def _update(x,y):
    '''x is any class instance, y is a dictionary'''
    for key in y.keys:
        x.__dict__[key] = y[key]
    else:
        x.timestamp = time.time()
    return x

class User():
    def __init__(self,name='any',user_id=1):
        self.name = str(name)
        self.user_id = int(user_id)
        return None

class Options():
    def __init__(self,opts):
        if not isinstance(opts,dict):
            raise AssertionError('Did not get options dictonary')
        self.__dict__ = opts
        self.timestamp = time.time()
        return None

    @clsmethod
    def update(cls,opts):
        return _update(cls,opts)
        
    @staticmethod
    def myOptions(options):
        if isinstance(options,Options):
            return options
        elif isinstance(options,dict):
            return Options(options)
        else:
            raise AssertionError('Did not get Options')

        
class Pipeline(User,Options):
    def __init__(self,user='',pipeline_id=1,software_root='',
                 data_root='',pipe_root='',configuration_root='',
                 description='',state_id=0,options=''):
        if not isinstance(user,User): raise AssertionError('Did not get User instance')
        User.__init__(user.name,user.user_id)
        self.pipeline_id = pipeline_id
        self.options = Options.myOptions(options)
        self.software_root = software_root
        self.data_root = data_root
        self.pipe_root = pipe_root
        self.configuration_root = configuration_root
        self.description = description
        self.state_id = state_id
        self.timestamp = time.time()
        return None
        
class Target(Pipeline):
    def __init__(self,name='',target_id=1,relativepath='',pipeline='',options=''):
        if not isinstance(name,str):
            raise AssertionError('Did not get target name as string')
        if not isinstance(pipeline,Pipeline):
            raise AssertionError('Did not get Pipeline instance')                
        self.name = name
        self.relativepath = str(relativepath)
        self.pipeline = pipeline
        self.pipeline_id = pipeline.pipeline_id
        self.target_id = int(target_id)
        self.options = pipeline.options
        if isinstance(options,dict): self.options.update(options)
        return None
        
class Configuration(Target):
    def __init__(self,name='',relativepath='',description='',target='',
                 config_id =1,options=''):
        if not isinstance(target,Target):
            raise AssertionError('Did not get Pipeline instance')
        self.name = name
        self.relativepath = str(relativepath)
        self.target = target
        self.target_id = target.target_id
        self.pipeline_id = target.pipeline_id
        self.config_id = int(config_id)
        self.description = description
        self.options = pipeline.options
        if isinstance(options,dict): self.options.update(options)
        return None
        
class DataProduct(Configuration):
    def __init__(self,filename,relativepath,group,configuration,
                 data_type='',subtype='',suffix='',filtername='',
                 ra=0,dec=0,pointing_angle=0,options=''):
    if not isinstance(configuration,Configuration):
        raise AssertionError('Did not get Configurations instance')

    self.configuration = configuration
    self.config_id = configuration.config_id
    self.target_id = configuration.target_id
    self.pipeline_id = configuration.pipeline_id
    self.dp_id = dp_id

    self.filename = str(filename)
    self.relativepath = str(relativepath)
    
    if '.' in filename:
        _suffix = filename.split('.')[-1]
        if _suffix in ['fits','txt','head','cl','py','pyc','pl',
                'phot','png','jpg','ps','gz','dat','lst','sh']:
            self.suffix = _suffix
        else:
            self.suffix = 'other'

    if not(data_type): self.data_type = str(self.suffix)
    self.subtype = str(subtype)
    
    if group in ['proc','conf','log','raw']:
        self.group = group
    else:
        self.group = str('other')
    
    self.filtername = str(filtername)
    self.ra = float(ra)
    self.dec = float(dec)
    self.pointing_angle = float(pointing_angle)

    self.options = configuration.options
    if isinstance(options,dict): self.options.update(options)
    
    self.timestamp = time.time()
    return None


class CopyState(DataProduct,Node):
    def __init__(self,dp='',node='',state='any',state_id=0):
        if not isinstance(dp,DataProduct):
            raise AssertionError('Did not get DataProduct instance')
        if not isinstance(node,Node):
            raise AssertionError('Did not get Node instance')
        self.state = state
        self.dp = dp
        self.dp_id = dp.dp_id
        self.pipeline_id = dp.pipeline_id
        self.target_id = dp.target_id
        self.config_id = dp.config_id
        self.node = node
        self.node_id = node.node_id
        return None
    
class Parameters(Configuration):
    def __init__(self,config='',params='',volatile=0):
        if not isinstance(configuration,Configuration):
            raise AssertionError('Did not get Configurations instance')
        if not isinstance(params,dict):
            raise AssertionError('Did not get Parameters dictionary')
        self.__dict__ = params
        self.config = config
        self.config_id = config.config_id
        self.target_id = config.target_id
        self.pipeline_id = config.pipeline_id
        self.volatile = bool(volatile)
        self.timestamp = time.time()
        return None

    @clsmethod
    def update(cls,params):
        return _update(cls,params)

    
class Task(Pipeline):
    def __init__(self,name='',task_id=1,flags='',
                 pipeline='',nruns=1,run_time=0,
                 is_exclusive=0,options=''):
        if not isinstance(name,str):
            raise AssertionError('Did not get target name as string')
        if not isinstance(pipeline,Pipeline):
            raise AssertionError('Did not get Pipeline instance')
        self.name = name
        self.pipeline = pipeline
        self.pipeline_id = pipeline.pipeline_id
        self.task_id = int(task_id)
        self.flags = str(flags)
        self.nruns = int(nruns)
        self.run_time = float(run_time)
        self.is_exclusive = bool(is_exclusive)
        self.options = pipeline.options
        if isinstance(options,dict): self.options.update(options)
        self.timestamp = time.time()
        
class Mask(Task):
    def __init__(self,task='',name='',value='',source='',optionflags=''):
        if not isinstance(task,Task):
            raise AssertionError('Did not get Task instance')

class TaskRequirements(Task):


class Node(User):
    

    
class Job(Task,Configuration,Node):
    def __init__(self,task='',config='',node='',
                 state='',pid=1,starttime=1,endtime=2,
                 options=''):
        
    




    
