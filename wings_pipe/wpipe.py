#! /usr/bin/env python
import time, os, sys
import numpy as np
import pandas as pd
from astropy import wcs
from astropy.io import fits, ascii
from astropy.table import Table


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

    
class Pipeline:


class DataProduct:


class Task:


class Mask:


class Configuration:


class Parameters:


class Job:
