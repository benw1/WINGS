#! /usr/bin/env python
import time, subprocess, os, psycopg2
import numpy as np
import pandas as pd

from sqlalchemy import create_engine, Column, Sequence, ForeignKey, func
from sqlalchemy import Integer, BigInteger, Float, String, DateTime, Boolean

from sqlalchemy.sql import select
from sqlalchemy.orm import sessionmaker, relationship, backref
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects import postgresql

from sqlalchemy_utils import database_exists, create_database, drop_database

username = 'rubab'
dbname = 'tempDB1'

engine = create_engine('postgres://%s@localhost/%s'%(username,dbname))

if not database_exists(engine.url):
    create_database(engine.url)
    
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

Base = declarative_base()


class BaseMixin(object):
    @classmethod
    def create(cls, *args, **kwargs):
        obj = cls(*args, **kwargs)
        session.add(obj)
        session.commit()


class Options(Base):
    __tablename__= 'options'
    
    opt_id = Column(BigInteger, Sequence('opt_id_seq'),
                     primary_key=True, nullable=False)

    name = Column(postgresql.VARCHAR(64),nullable=False)
    value = Column(postgresql.VARCHAR(64),nullable=False)
    

class User(BaseMixin,Base):
    __tablename__= 'users'
    user_id = Column(Integer, Sequence('user_id_seq'),
                     primary_key=True, nullable=False)
    name = Column(postgresql.VARCHAR(32),nullable=False)
    timestamp = Column(DateTime, default=func.now())

    def __init__(self,name='any'):
        self.name = str(name)
        
    def get(user_name):
        with engine.connect() as conn:
            _df = pd.read_sql_query(select([User])
                    .where(User.name==str(user_name))
                    ,conn)
        return _df
    
class Pipeline(BaseMixin,Base):
    __tablename__= 'pipelines'
    pipeline_id = Column(Integer, Sequence('pipeline_id_seq'),
                     primary_key=True, nullable=False)
    name = Column(postgresql.VARCHAR(64),nullable=False)
    
    user_id = Column(Integer, ForeignKey('users.user_id'))
    user = relationship(User,
                        backref=backref('pipelines',
                        uselist=True,passive_updates=False,
                        cascade='delete,all'))
    
    software_root = Column(postgresql.VARCHAR(256))
    data_root = Column(postgresql.VARCHAR(256))
    pipe_root = Column(postgresql.VARCHAR(256))
    config_root = Column(postgresql.VARCHAR(256))
    description = Column(postgresql.VARCHAR(512))
    timestamp = Column(DateTime, default=func.now())

    def __init__(self,user,name='any',software_root='',
                 data_root='',pipe_root='',config_root='',
                 description=''):
        user = user.iloc[0]
        self.name = str(name)
        self.user_name = str(user['name'])
        self.user_id = int(user.user_id)      
        self.software_root = str(software_root)
        self.data_root = str(data_root)
        self.pipe_root = str(pipe_root)
        self.config_root = str(config_root)
        self.description = str(description)
        
    def get(pipeline_id):
        with engine.connect() as conn:
            _df = pd.read_sql_query(select([Pipeline])
                    .where(Pipeline.pipeline_id==int(pipeline_id))
                    ,conn)
        return _df
    
class Target(BaseMixin,Base):
    __tablename__= 'targets'
    
    target_id = Column(Integer, Sequence('target_id_seq'),
                       primary_key=True, nullable=False)
    name = Column(postgresql.VARCHAR(64),nullable=False)
    
    user_id = Column(Integer, ForeignKey('users.user_id'))
    
    pipeline_id = Column(Integer, ForeignKey('pipelines.pipeline_id'))
    pipeline = relationship(Pipeline,
                        backref=backref('targets',
                        uselist=True,passive_updates=False,
                        cascade='delete,all'))
    
    opt_id = Column(Integer, ForeignKey('options.opt_id'))
    options = relationship(Options,
                           backref=backref('targets',
                           uselist=True,passive_updates=False,
                           cascade='delete,all'))
    
    relativepath = Column(postgresql.VARCHAR(256))
    
    timestamp = Column(DateTime, default=func.now())
    
    
    def __init__(self,name,pipeline,create_dir=False):
        pipeline = pipeline.iloc[0]
        self.name = str(name)
        self.user_id = int(pipeline.user_id)
        self.pipeline_id = int(pipeline.pipeline_id)
        self.relativepath = str(pipeline.data_root)+'/'+str(name)
        if create_dir:
            _t = subprocess.run(['mkdir', '-p', str(self.relativepath)],
                                stdout=subprocess.PIPE)
        
    def get(target_id):
        with engine.connect() as conn:
            _df = pd.read_sql_query(select([Target])
                    .where(Target.target_id==int(target_id))
                    ,conn)
        return _df

class Configuration(BaseMixin,Base):
    __tablename__= 'configurations'
    
    config_id = Column(Integer, Sequence('config_id_seq'),
                     primary_key=True, nullable=False)
    name = Column(postgresql.VARCHAR(64),nullable=False)
    
    user_id = Column(Integer, ForeignKey('users.user_id'))
    pipeline_id = Column(Integer, ForeignKey('pipelines.pipeline_id'))
    
    target_id = Column(Integer, ForeignKey('targets.target_id'))
    target = relationship(Target,
                        backref=backref('configurations',
                        uselist=True,passive_updates=False,
                        cascade='delete,all'))
    
    relativepath = Column(postgresql.VARCHAR(256))
    logpath = Column(postgresql.VARCHAR(256))
    confpath = Column(postgresql.VARCHAR(256))
    rawpath = Column(postgresql.VARCHAR(256))
    procpath = Column(postgresql.VARCHAR(256))
    description = Column(postgresql.VARCHAR(512))
    
    timestamp = Column(DateTime, default=func.now())

    def __init__(self,name,description,target,create_dir=False):
        target = target.iloc[0]
        self.name = str(name)
        self.relativepath = str(target.relativepath)
        self.logpath = str(target.relativepath)+'/log_'+str(name)
        self.confpath = str(target.relativepath)+'/conf_'+str(name)
        self.rawpath = str(target.relativepath)+'/raw_'+str(name)
        self.procpath = str(target.relativepath)+'/proc_'+str(name)
        self.user_id = int(target.user_id)
        self.target_id = int(target.target_id)
        self.pipeline_id = int(target.pipeline_id)
        self.description = str(description)
        
        if create_dir:
            for _path in [self.rawpath,self.confpath,self.procpath,self.logpath]:
                _t = subprocess.run(['mkdir', '-p', str(_path)], stdout=subprocess.PIPE)
                
    def get(config_id):
        with engine.connect() as conn:
            _df = pd.read_sql_query(select([Configuration])
                    .where(Configuration.config_id==int(config_id))
                    ,conn)
        return _df    
    
class Parameters(Base):
    __tablename__= 'parameters'
    
    param_id = Column(BigInteger, Sequence('param_id_seq'),
                     primary_key=True, nullable=False)

    config_id = Column(Integer, ForeignKey('configurations.config_id'))
    configuration = relationship(Configuration,
                        backref=backref('parameters',
                        uselist=True,passive_updates=False,
                        cascade='delete,all'))
                        
    name = Column(postgresql.VARCHAR(64),nullable=False)
    value = Column(postgresql.VARCHAR(64),nullable=False)

    def getParam(config_id):
        with engine.connect() as conn:
            _df = pd.read_sql_query(select([Parameters])
                    .where(Parameters.config_id==int(config_id))
                    ,conn)
        return dict(zip([_df['name']],[_df['value']]))
    
    
class DataProduct(BaseMixin,Base):
    __tablename__= 'data_products'
    
    dp_id = Column(BigInteger, Sequence('dp_id_seq'),
                     primary_key=True, nullable=False)
                     
    user_id = Column(Integer, ForeignKey('users.user_id'))
    pipeline_id = Column(Integer, ForeignKey('pipelines.pipeline_id'))
    target_id = Column(Integer, ForeignKey('targets.target_id'))
    
    config_id = Column(Integer, ForeignKey('configurations.config_id'))
    configuration = relationship(Configuration,
                        backref=backref('data_products',
                        uselist=True,passive_updates=False,
                        cascade='delete,all'))
    
    filename = Column(postgresql.VARCHAR(128))
    relativepath = Column(postgresql.VARCHAR(256))
    suffix = Column(postgresql.VARCHAR(8))
    data_type = Column(postgresql.VARCHAR(16))
    subtype = Column(postgresql.VARCHAR(16))
    group = Column(postgresql.VARCHAR(8))
    filtername = Column(postgresql.VARCHAR(8))
    
    ra = Column(Float)
    dec = Column(Float)
    pointing_angle = Column(Float)
    
    opt_id = Column(Integer, ForeignKey('options.opt_id'))
    options = relationship(Options,
                           backref=backref('data_products',
                           uselist=True,passive_updates=False,
                           cascade='delete,all'))    
    
    timestamp = Column(DateTime, default=func.now())
    
    def __init__(self,filename,relativepath,group,
                 configuration,
                 data_type='',subtype='',filtername='',
                 ra=0,dec=0,pointing_angle=0):
        configuration = configuration.iloc[0]
        self.user_id = int(configuration.user_id)
        self.config_id = int(configuration.config_id)
        self.target_id = int(configuration.target_id)
        self.pipeline_id = int(configuration.pipeline_id)

        self.filename = str(filename)
        self.relativepath = str(relativepath)

        _suffix = ' '
        if '.' in filename:
            _suffix = filename.split('.')[-1]
        if _suffix not in ['fits','txt','head','cl',
           'py','pyc','pl','phot','png','jpg','ps',
           'gz','dat','lst','sh']:
            _suffix = 'other'                                      
        self.suffix = str(_suffix)
        
        if not(data_type): data_type = _suffix
        self.data_type = str(data_type)
        self.subtype = str(subtype)

        if group not in ['proc','conf','log','raw']:
            group = 'other'
        self.group = str('other')

        self.filtername = str(filtername)
        self.ra = float(ra)
        self.dec = float(dec)
        self.pointing_angle = float(pointing_angle)


    def get(dp_id):
        with engine.connect() as conn:
            _df = pd.read_sql_query(select([DataProduct])
                    .where(DataProduct.dp_id==int(dp_id))
                    ,conn)
        return _df
    
class Task(BaseMixin,Base):
    __tablename__= 'tasks'
    
    task_id = Column(Integer, Sequence('task_id_seq'),
                     primary_key=True, nullable=False)
    
    name = Column(postgresql.VARCHAR(64),nullable=False)
    
    user_id = Column(Integer, ForeignKey('users.user_id'))
    
    pipeline_id = Column(Integer, ForeignKey('pipelines.pipeline_id'))
    pipeline = relationship(Pipeline,
                        backref=backref('tasks',
                        uselist=True,passive_updates=False,
                        cascade='delete,all'))
    
    nruns = Column(Float)
    runtime = Column(Float)
    is_exclusive = Column(Boolean)
    
    timestamp = Column(DateTime, default=func.now())
    
    def __init__(self,name,
                 pipeline,
                 nruns=0,run_time=0,
                 is_exclusive=0):
        pipeline = pipeline.iloc[0]
        self.name = str(name)
        
        self.user_id = int(pipeline.user_id)
        self.pipeline_id = int(pipeline.pipeline_id)
        self.nruns = int(nruns)
        self.run_time = float(run_time)
        self.is_exclusive = bool(is_exclusive)
        
    def get(task_id):
        with engine.connect() as conn:
            _df = pd.read_sql_query(select([Task])
                    .where(Task.task_id==int(task_id))
                    ,conn)
        return _df
    
class Mask(BaseMixin,Base):
    __tablename__= 'masks'
    
    mask_id = Column(Integer, Sequence('mask_id_seq'),
                     primary_key=True, nullable=False)
    
    task_id = Column(Integer, ForeignKey('tasks.task_id'))
    task = relationship(Task,
                        backref=backref('masks',
                        uselist=True,passive_updates=False,
                        cascade='delete,all'))
    
    source = Column(postgresql.VARCHAR(64),nullable=False)
    name = Column(postgresql.VARCHAR(64),nullable=False)
    value = Column(postgresql.VARCHAR(64),nullable=False)
    
    timestamp = Column(DateTime, default=func.now())
    
    
    def __init__(self,task,source='',name='',value=''):
        task = task.iloc[0]
        self.source = str(source)
        self.name   = str(name)
        self.value  = str(value)
        self.task_id = int(task.task_id)
        
        
    def get(mask_id):
        with engine.connect() as conn:
            _df = pd.read_sql_query(select([Mask])
                    .where(Mask.mask_id==int(mask_id))
                    ,conn)
        return _df

    
class Job(BaseMixin,Base):
    __tablename__= 'jobs'
    
    job_id = Column(BigInteger, Sequence('job_id_seq'),
                     primary_key=True, nullable=False)
    
    user_id = Column(Integer, ForeignKey('users.user_id'))
    
    pipeline_id = Column(Integer, ForeignKey('pipelines.pipeline_id'))
    
    task_id = Column(Integer, ForeignKey('tasks.task_id'))
    task = relationship(Task,
                        backref=backref('jobs',
                        uselist=True,passive_updates=False,
                        cascade='delete,all'))
    
    config_id = Column(Integer, ForeignKey('configurations.config_id'))
    configuration = relationship(Configuration,
                        backref=backref('jobs',
                        uselist=True,passive_updates=False,
                        cascade='delete,all'))
    
    opt_id = Column(Integer, ForeignKey('options.opt_id'))
    options = relationship(Options,
                           backref=backref('jobs',
                           uselist=True,passive_updates=False,
                           cascade='delete,all'))
    
    # This is for the parent's event_id
    event_id = Column(Integer, ForeignKey('events.event_id'))
    
    # This is for the child events
    event = relationship('Event', secondary='job_event_link')
    
    node_id = Column(Integer, ForeignKey('nodes.node_id'))
    node = relationship('Node', secondary='job_node_link')
    
    state = Column(postgresql.VARCHAR(64),nullable=False)
    starttime = Column(DateTime, default=func.now())
    endtime = Column(DateTime, default=func.now())
    
    timestamp = Column(DateTime, default=func.now())
    
    
    def __init__(self,state,event_id,
                 task,
                 config,
                 node):
        self.state = str(state)
        self.event_id = int(event_id)
        task,config,node = task.iloc[0],config.iloc[0],node.iloc[0]
        self.task_id = int(task.task_id)
        self.config_id = int(config.config_id)
        self.node_id = int(node.node_id)
        self.pipeline_id =  int(config.pipeline_id)
        self.user_id = int(task.user_id)
    
    
    def get(job_id):
        with engine.connect() as conn:
            _df = pd.read_sql_query(select([Job])
                    .where(Job.job_id==int(job_id))
                    ,conn)
        return _df
    
class Event(BaseMixin,Base):
    __tablename__= 'events'
    
    event_id = Column(BigInteger, Sequence('event_id_seq'),
                     primary_key=True, nullable=False)
    
    # Job that created this event
    job_id = Column(Integer, ForeignKey('jobs.job_id'))
    job = relationship(Job, secondary='job_event_link')
    
    jargs = Column(postgresql.VARCHAR(64),nullable=False)
    name = Column(postgresql.VARCHAR(64),nullable=False)
    value = Column(postgresql.VARCHAR(64),nullable=False)
    
    opt_id = Column(Integer, ForeignKey('options.opt_id'))
    options = relationship(Options,
                           backref=backref('events',
                           uselist=True,passive_updates=False,
                           cascade='delete,all'))
    
    timestamp = Column(DateTime, default=func.now())
    
    def __init__(self,name,value,jargs,job):
        job = job.iloc[0]
        self.job_id = int(job.job_id)
        self.jargs  = str(jargs)
        self.name   = str(name)
        self.value  = str(value)
        
    def get(event_id):
        with engine.connect() as conn:
            _df = pd.read_sql_query(select([Event])
                    .where(Event.event_id==int(event_id))
                    ,conn)
        return _df
    
class Node(BaseMixin,Base):
    __tablename__= 'nodes'
    
    node_id = Column(Integer, Sequence('node_id_seq'),
                     primary_key=True, nullable=False)
    name = Column(postgresql.VARCHAR(64),nullable=False)
    
    job_id = Column(Integer, ForeignKey('jobs.job_id'))
    job = relationship(Job, secondary='job_node_link')
    
    int_ip = Column(postgresql.INET)
    ext_ip = Column(postgresql.INET)
    
    timestamp = Column(DateTime, default=func.now())
    
    def __init__(self,name='any',int_ip='',ext_ip=''):
        self.name = str(name)
        self.int_ip = int_ip
        self.ext_ip = ext_ip
        
    def get(node_id):
        with engine.connect() as conn:
            _df = pd.read_sql_query(select([Node])
                    .where(Node.node_id==int(node_id))
                    ,conn)
        return _df
    
class JobEventLink(Base):
    __tablename__ = 'job_event_link'    
    job_id = Column(Integer, ForeignKey('jobs.job_id'), primary_key=True)
    event_id = Column(Integer, ForeignKey('events.event_id'), primary_key=True)

    
class JobNodeLink(Base):
    __tablename__ = 'job_node_link'    
    job_id = Column(Integer, ForeignKey('jobs.job_id'), primary_key=True)
    node_id = Column(Integer, ForeignKey('nodes.node_id'), primary_key=True)

Base.metadata.create_all(engine)

session.commit()
