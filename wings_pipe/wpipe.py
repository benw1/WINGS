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
    def __init__(self,**params):
        self.name = params['name']
        self.user_id = params['user_id']
        return None

class Options():
    def __init__(self,**opts):
        if not isinstance(opts,dict):
            raise AssertionError('Did not get options dictonary')
        opts['timestamp'] = time.time()
        self.__dict__ = opts
        return None

    @clsmethod
    def update(cls,**opts):
        return _update(cls,opts)
        
    @staticmethod
    def myOptions(options):
        if isinstance(options,Options):
            return options
        elif isinstance(options,dict):
            return Options(options)
        else:
            return Options()

        
class Pipeline(User,Options):
    def __init__(self,user='any',pipeline_id=0,options=''):
        if not isinstance(user,User): raise AssertionError('Did not get User instance')
        self.user = User.__init__(user.name,user.user_id)
        self.pipeline_id = pipeline_id
        self.options = Options.myOptions(options)
        self.timestamp = time.time()
        return None
        
class Target(Pipeline):
    def __init__(self,name='',relativepath='',pipeline='',options=''):
        if not isinstance(name,str):
            raise AssertionError('Did not get target name as string')
        if not isinstance(pipeline,Pipeline):
            raise AssertionError('Did not get Pipeline instance')                
        self.name = name
        self.pipeline = pipeline
        self.relativepath = str(relativepath)
        self.options = pipeline.options
        if isinstance(options,dict): self.options.update(options)
        return None
        
class Configuration(Target):
    def __init__(self,target='',description=''):
        if not isinstance(pipeline,Pipeline):
            raise AssertionError('Did not get Target instance')
        self.target = target
        self.description = description
        self.relativepath = str(relativepath)
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

    self.filename = str(filename)

    if '.' in filename:
        _suffix = filename.split('.')[-1]
        if _suffix in ['fits','txt','head','cl','py','pyc','pl',
                'phot','png','jpg','ps','gz','dat','lst','sh']:
            self.suffix = _suffix
        else:
            self.suffix = 'other'

    if data_type=='':
        self.data_type = str(self.suffix)

    self.subtype = str(subtype)
    
    if group in ['proc','conf','log','raw']:
        self.group = str(group)
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


class CopyState(DataProduct):


class Parameters(Configuration):
    
    
class Task(Pipeline):


class Mask(Task):


class Requirements(Task):
    
    
class Job(Task,Configuration):


class Event(Job):
