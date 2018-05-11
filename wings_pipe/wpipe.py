#! /usr/bin/env python
import time, os, sys
import numpy as np
import pandas as pd
from astropy import wcs
from astropy.io import fits, ascii
from astropy.table import Table

class Option(self,blah=1):
    self.blah = blah

class Target:
    def __init__(self,pipeline,config,name='',relativepath=''):
        self.name = name
        self.relativepath = relativepath
        self.pipeline = pipeline
        self.pipeline_id = pipeline.id
        self.configuration = config

    def new():


    def create():


    def edit():


    def destroy():


    def newjob():


    def startjob():

    
class Pipeline(user):

class Configuration:
    
class DataProduct(Option):
    def __init__(self,relativepath,filename,group,configuration,
                 data_type='',subtype='',suffix='',filtername='',
                 ra=0,dec=0,pointing_angle=0):
    
    self.relativepath = str(relativepath)
    self.filename = str(filename)
    self.subtype = str(subtype)
    self.filtername = str(filtername)
    self.ra = float(ra)
    self.dec = float(dec)
    self.pointing_angle = float(pointing_angle)
    
    
    if group in ['proc','conf','log','raw']:
        self.group = str(group)
    else:
        raise ValueError('Invalid group name')

    if type(configuration) is Configuration:
        self.configuration = configuration
    else:
        raise ValueError('Did not get Configuration object')

    if '.' in filename:
        _suffix = filename.split('.')[-1]
        if _suffix in ['fits','txt','head','cl','py','pyc','pl','phot','png','jpg','ps','gz','dat','lst','sh']:
            self.suffix	= _suffix
        else:
            self.suffix = 'other'

    if data_type=='': 
        self.data_type = str(self.suffix)


    self.timestamp = time.time()

        
    def new():

    def create():

    def delete():

    def update():

    def create_or_update():

        
        
    
class Task:


class Mask:


class Configuration:


class Parameters:


class Job:
