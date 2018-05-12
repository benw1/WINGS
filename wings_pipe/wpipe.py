#! /usr/bin/env python
import time, os, sys
import numpy as np
import pandas as pd
from astropy import wcs
from astropy.io import fits, ascii
from astropy.table import Table


def _update(x,y):
    '''x is any class instance, y is a dictionary'''
    for key in y.keys:
        x.__dict__[key] = y[key]
    else:
        x.timestamp = time.time()
    return x


class Options():
    def __init__(self,opts={'blah': 1}):
        if not isinstance(opts,dict):
            raise AssertionError('Did not get options dictonary')
        opts['timestamp'] = time.time()
        self.__dict__ = opts
        return None

    @clsmethod
    def update(cls,opts={'blah': 1}):
        return _update(cls,opts)
        
    @staticmethod
    def myOptions(options):
        if isinstance(options,Options):
            return options
        elif isinstance(options,dict):
            return Options(options)
        else:
            return Options()

        
class Pipeline(Options):
    def __init__(self,user='any',pipeline_id=0,options=''):            
        self.user = user
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
        
class Configurations(Target):
    def __init__(self,target='',description='',**params):
        if not isinstance(pipeline,Pipeline):
            raise AssertionError('Did not get Target instance')
        self.target = target
        self.description = description
        self.relativepath = str(relativepath)
        self.params = params
        self.options = pipeline.options
        if isinstance(options,dict): self.options.update(options)
        return None
        
class DataProduct(Configurations):
    def __init__(self,filename,relativepath,group,configuration,
                 data_type='',subtype='',suffix='',filtername='',
                 ra=0,dec=0,pointing_angle=0,options=''):
    if not isinstance(configuration,Configurations):
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


class Parameters:
        
    
class Task:


class Mask:


class Job:
