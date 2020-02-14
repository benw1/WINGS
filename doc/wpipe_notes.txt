#! /usr/bin/env python
'''
Make things go upstream??

'''


import time, os, sys, json
import numpy as np
import pandas as pd
from astropy import wcs
from astropy.io import fits, ascii
from astropy.table import Table

def to_dict(x):
    '''x is any class instance'''
    return x.__dict__

def to_json(x):
    '''x is any class instance'''
    return json.dumps(x.__dict__)

def _update(x,y):
    '''x is any class instance, y is a dictionary'''
    for key in y.keys:
        x.__dict__[key] = y[key]
    else:
        x.timestamp = time.time()
    return x
'''
def increment(x):
    return int(x)+1
'''

class User():
    def __init__(self,name='any',user_id=1):
        self.name = str(name)
        self.user_id = int(user_id)
        return None

    # create user
    # delete user
    
class Node():
    def __init__(self,name='any',node_id=1,int_ip='',ext_ip=''):
        self.name = str(name)
        self.node_id = int(node_id)
        self.int_ip = int_ip
        self.ext_ip = ext_ip
        return None

    
class Options():
    def __init__(self,opts):
        if not isinstance(opts,dict):
            raise AssertionError('Did not get options dictonary')
        self.__dict__ = opts
        self.timestamp = time.time()
        return None

    #@clsmethod
    def add_or_update(cls,opts):
        return _update(cls,opts)
        
    #@staticmethod
    def myOptions(options):
        if isinstance(options,Options):
            return options
        elif isinstance(options,dict):
            return Options(options)
        else:
            raise AssertionError('Did not get Options')

        
class Pipeline(User,Options):
    def __init__(self,user='',name='',pipeline_id=1,software_root='',
                 data_root='',pipe_root='',configuration_root='',
                 description='',state_id=0,options=''):
        if not isinstance(user,User): raise AssertionError('Did not get User instance')
        self.name = name
        self.user = user
        self.user_id = user.user_id
        self.pipeline_id = pipeline_id
        self.options = Options.myOptions(options)
        self.software_root = software_root
        self.data_root = data_root
        self.pipe_root = pipe_root
        self.configuration_root = configuration_root
        self.description = description
        # self.state
        self.state_id = state_id
        self.timestamp = time.time()
        return None

    def create(self):
        
    
    def update(self):


    def duplicate(self):


    def delete(self):
        

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

    # create
    # update
    # duplicate

    
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
        self.options = target.options
        if isinstance(options,dict): self.options.update(options)
        return None
        
class DataProduct(Configuration):
    def __init__(self,filename='',relativepath='',group='',configuration='',
                 dp_id=1,data_type='',subtype='',suffix='',filtername='',
                 ra=0,dec=0,pointing_angle=0,options='',**tags):
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
        self.tags = Options(tags) # meant to break
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
        if not isinstance(config,Configuration):
            raise AssertionError('Did not get Configurations instance')
        if not isinstance(params,dict):
            raise AssertionError('Did not get Parameters dictionary')
        self.__dict__ = params
        self.config = config
        self.config_id = config.config_id
        self.target_id = config.target_id
        self.pipeline_id = config.pipeline_id
        self.timestamp = time.time()
        return None

    #@clsmethod
    def update(cls,params):
        return _update(cls,params)

    # Initialize parameters by readingb in from file
    # Add (or update) parameters
    

    
    
class Task(Pipeline):
    def __init__(self,name='',task_id=1,flags='',
                 pipeline='',nruns=1,run_time=0,
                 is_exclusive=0,mask=[],options=''):
        if not isinstance(name,str):
            raise AssertionError('Did not get target name as string')
        if not isinstance(pipeline,Pipeline):
            raise AssertionError('Did not get Pipeline instance')
        self.name = name
        self.pipeline = pipeline
        self.pipeline_id = pipeline.pipeline_id
        self.task_id = int(task_id)
        self.options = pipeline.options
        if isinstance(options,dict): self.options.update(options)
        self.mask = mask
        self.timestamp = time.time()
        return None
    
    def addMask(self,mask=[]):
        if not isinstance(mask,Mask): raise AssertionError('Did not get Mask instance')
        self.mask.append(mask)


''' Rename Mask to something else'''
class Mask():
    def __init__(self,source='',name='',value=''):
        if not(source): raise AssertionError('Did not get source')
        if not(name): raise AssertionError('Did not get name')
        if not(value): raise AssertionError('Did not get value')
        # 'source' has to be an existing task_name for this pipeline or wildcard
        self.source = source
        self.name = name
        self.value = value
        return None

    
'''
class Job(Task,Configuration,Node):
    def __init__(self,task='',config='',node='',
                 state='',pid=1,starttime=1,endtime=2,
                 options=''):
        
    
class Event(self,job='',name='',value='',options=''):
    # Masks & events get 'compared'


    
class Node(User):
''' 

def get_dp(config,use_and=True,**kwargs):
    if not isinstance(config,Configuration):
        raise AssertionError('Did not get Configurations instance')
    all_dp = '' # load all data products for the config_id given
    
    if use_and:
        _dp = all_dp
        for key in kwargs.keys:
            _dp = _dp[_dp.key==kwargs[key]]
    else:
        _dp = []
        for key in kwargs.keys:
            _dp1 = all_dp[all_dp.key==kwargs[key]]
            _dp.append(_dp1)
        else:
            pass # Remove duplicates
    return _dp

def get_unique_filters(config,dp_group):
    '''
    Return a list of all filters, non-repeatitive, that have been used
    in a in data-product of the given group wihin given config
    '''


def get_path_for(config,filename='',group=''):
    return config.target.pipeline.pipe_root+'/'+config.target.relativepath+'/'+group+'/'+filename


def add_params(config):


def get_params(config):


def add_value(dp):
    ''' Any key-value pair '''

def set_value(dp):


def increment(opt):
    ''' 
    Increment opt by 1 in DB and return incremented value
    '''
    opt += 1
    return opt

def create_event(job,name='',value='',options):


def fire_event(event):
    '''
    Look into app/models/event.rb
    First get event.job.task.pipeline
    Then find all masks matching the event
    Then start the associated tasks
    '''
    
def get_option_value(X,opt):
    return X.options[opt]

def register_task():
    



