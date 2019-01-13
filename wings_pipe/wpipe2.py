#! /usr/bin/env python
import time, subprocess, os
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

class BaseMixin():
    def create(self):
        session.add(self)
        session.commit()
        return self
    
    def getOpt(self,how='sql'):
        rs = self.options
        return rs

class Options(Base):
    __tablename__= 'options'
    
    opt_id = Column(BigInteger, Sequence('opt_id_seq'),
                     primary_key=True, nullable=False)

    target_id = Column(Integer, ForeignKey('targets.target_id'))
    dp_id     = Column(Integer, ForeignKey('data_products.dp_id'))
    job_id    = Column(Integer, ForeignKey('jobs.job_id'))
    event_id  = Column(Integer, ForeignKey('events.event_id'))
    
    name = Column(postgresql.VARCHAR(64),nullable=False)
    value = Column(postgresql.VARCHAR(64),nullable=False)
    
    def __init__(self, name='any', value=0):
        self.name = str(name)
        self.value = str(value)
        
    def create(self,opt={'any':1}):
        opts = []
        for item in opt.items():
            opts.append(Options(item[0],item[1]))
        session.add_all(opts)
        session.commit()
        return opts

class User(BaseMixin,Base):
    __tablename__= 'users'
    user_id = Column(Integer, Sequence('user_id_seq'),
                     primary_key=True, nullable=False)
    name = Column(postgresql.VARCHAR(32),nullable=False)
    
    pipelines = relationship('Pipeline',
                    backref=backref('users',
                    uselist=True,passive_updates=False,
                    cascade='delete,all'))
    
    timestamp = Column(DateTime, default=func.now())
    
    def __init__(self,name='any'):
        self.name = str(name)

    def add_pipeline(self,obj):
        self.pipelines.append(obj)
        session.commit()
        return
    
    @staticmethod
    def get(user_name,how='sql'):
        if how=='sql':
            rs = session.query(User)\
            .filter_by(name=str(user_name)).one()
        elif how=='pd':
            with engine.connect() as conn:
                rs = pd.read_sql_query(select([User])
                    .where(User.name==str(user_name))
                           ,conn)
                rs = rs.iloc[0]
        return rs
    

class Pipeline(BaseMixin,Base):
    __tablename__= 'pipelines'
    pipeline_id = Column(Integer, Sequence('pipeline_id_seq'),
                     primary_key=True, nullable=False)
    name = Column(postgresql.VARCHAR(64),nullable=False)
    
    user_id = Column(Integer, ForeignKey('users.user_id'))
    
    targets = relationship('Target',
                        backref=backref('pipelines',
                        uselist=True,passive_updates=False,
                        cascade='delete,all'))
    
    tasks = relationship('Task',
                        backref=backref('pipelines',
                        uselist=True,passive_updates=False,
                        cascade='delete,all'))
    
    software_root = Column(postgresql.VARCHAR(256))
    data_root = Column(postgresql.VARCHAR(256))
    pipe_root = Column(postgresql.VARCHAR(256))
    config_root = Column(postgresql.VARCHAR(256))
    description = Column(postgresql.VARCHAR(512))
    timestamp = Column(DateTime, default=func.now())

    def __init__(self,name='any',software_root='',
                 data_root='',pipe_root='',config_root='',
                 description=''):
        self.name = str(name)  
        self.software_root = str(software_root)
        self.data_root = str(data_root)
        self.pipe_root = str(pipe_root)
        self.config_root = str(config_root)
        self.description = str(description)
    
    def add_target(self,obj,create_dir=False):
        self.targets.append(obj)
        obj.add_paths(self.pipeline_id,
                      create_dir)
        session.commit()
        return
    
    def add_task(self,obj):
        self.tasks.append(obj)
        session.commit()
        return
                                           
    @staticmethod        
    def get(pipeline_id,how='sql'):
        if how=='sql':            
            rs = session.query(Pipeline).get(int(pipeline_id))
        elif how=='pd':
            with engine.connect() as conn:
                rs = pd.read_sql_query(select([Pipeline])
                        .where(Pipeline.pipeline_id==int(pipeline_id))
                        ,conn)
                rs = rs.iloc[0]
        return rs


class Target(BaseMixin,Base):
    __tablename__= 'targets'
    
    target_id = Column(Integer, Sequence('target_id_seq'),
                       primary_key=True, nullable=False)
    name = Column(postgresql.VARCHAR(64),nullable=False)
    
    pipeline_id = Column(Integer, ForeignKey('pipelines.pipeline_id'))
    
    configurations = relationship('Configuration',
                        backref=backref('targets',
                        uselist=True,passive_updates=False,
                        cascade='delete,all'))  
    
    options = relationship('Options',
                           backref=backref('targets',
                           uselist=True,passive_updates=False,
                           cascade='delete,all'))
    
    relativepath = Column(postgresql.VARCHAR(256))
    
    timestamp = Column(DateTime, default=func.now())
    
    def __init__(self,name='any'):
        self.name = str(name)
        
    def add_config(self,obj,create_dir=False):
        self.configurations.append(obj)
        obj.add_paths(self.target_id,
                      create_dir)
        session.commit()
        return
    
    def add_options(self,obj):
        for opt in obj:
            self.options.append(opt)
        session.commit()
        return
    
    def add_paths(self,pipeline_id,create_dir=False):
        pipeline = Pipeline.get(int(pipeline_id))
        self.relativepath = str(pipeline.data_root)+'/'+str(self.name)
        if create_dir:
            _t = subprocess.run(['mkdir', '-p', str(self.relativepath)],
                                stdout=subprocess.PIPE)
        return
    
    @staticmethod
    def get(target_id,how='sql'):
        if how=='sql':
            rs = session.query(Target).get(int(target_id))
        elif how=='pd':
            with engine.connect() as conn:
                rs = pd.read_sql_query(select([Target])
                        .where(Target.target_id==int(target_id))
                        ,conn)
                rs = rs.iloc[0]
        return rs
           
class Configuration(BaseMixin,Base):
    __tablename__= 'configurations'
    
    config_id = Column(Integer, Sequence('config_id_seq'),
                     primary_key=True, nullable=False)
    name = Column(postgresql.VARCHAR(64),nullable=False)
    
    target_id = Column(Integer, ForeignKey('targets.target_id'))
    
    data_products = relationship('DataProduct',
                           backref=backref('configurations',
                           uselist=True,passive_updates=False,
                           cascade='delete,all'))
    
    parameters = relationship('Parameters',
                           backref=backref('configurations',
                           uselist=True,passive_updates=False,
                           cascade='delete,all')) 
    
    jobs = relationship('Job',
                    backref=backref('configurations',
                    uselist=False,passive_updates=False))
    
    relativepath = Column(postgresql.VARCHAR(256))
    logpath = Column(postgresql.VARCHAR(256))
    confpath = Column(postgresql.VARCHAR(256))
    rawpath = Column(postgresql.VARCHAR(256))
    procpath = Column(postgresql.VARCHAR(256))
    description = Column(postgresql.VARCHAR(512))
    
    timestamp = Column(DateTime, default=func.now())

    def __init__(self,name='any',description=''):
        self.name = str(name)
        self.description = str(description)

    def add_dp(self,obj):
        self.data_products.append(obj)
        session.commit()
        return

    def add_params(self,obj):
        for param in obj:
            self.parameters.append(param)
        session.commit()
        return
    
    def add_paths(self,target_id,create_dir=False):
        target = Target.get(int(target_id))
        self.relativepath = str(target.relativepath)
        self.logpath = str(target.relativepath)+'/log_'+str(self.name)
        self.confpath = str(target.relativepath)+'/conf_'+str(self.name)
        self.rawpath = str(target.relativepath)+'/raw_'+str(self.name)
        self.procpath = str(target.relativepath)+'/proc_'+str(self.name)
        
        if create_dir:
            for _path in [self.rawpath,self.confpath,self.procpath,self.logpath]:
                _t = subprocess.run(['mkdir', '-p', str(_path)], stdout=subprocess.PIPE)        
        return
    
    def add_job(self,obj):
        self.jobs.append(obj)
        session.commit()
        return
    
    @staticmethod            
    def get(config_id,how='sql'):
        if how=='sql':
            rs = session.query(Configuration).get(int(config_id))
        elif how=='pd':
            with engine.connect() as conn:
                rs = pd.read_sql_query(select([Configuration])
                        .where(Configuration.config_id==int(config_id))
                        ,conn)
                rs = rs.iloc[0]
        return rs    
    
    
class Parameters(Base):
    __tablename__= 'parameters'
    
    param_id = Column(BigInteger, Sequence('param_id_seq'),
                     primary_key=True, nullable=False)

    config_id = Column(Integer, ForeignKey('configurations.config_id'))
                        
    name = Column(postgresql.VARCHAR(64),nullable=False)
    value = Column(postgresql.VARCHAR(64),nullable=False)
    
    def __init__(self, name='any', value=0):
        self.name = str(name)
        self.value = str(value)
        
    def create(self,param={'any':1}):
        params = []
        for item in param.items():
            params.append(Parameters(item[0],item[1]))
        session.add_all(params)
        session.commit()
        return params
    
    @staticmethod
    def getParams(config_id):
        with engine.connect() as conn:
            _df = pd.read_sql_query(select([Parameters])
                    .where(Parameters.config_id==int(config_id))
                    ,conn)
        return dict(zip(_df['name'],_df['value']))
    
    
class DataProduct(BaseMixin,Base):
    __tablename__= 'data_products'
    
    dp_id = Column(BigInteger, Sequence('dp_id_seq'),
                     primary_key=True, nullable=False)
    
    config_id = Column(Integer, ForeignKey('configurations.config_id'))
    
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
    
    options = relationship('Options',
                           backref=backref('data_products',
                           uselist=True,passive_updates=False,
                           cascade='delete,all'))    
    
    timestamp = Column(DateTime, default=func.now())
    
    def __init__(self,filename='',relativepath='',group='',
                 data_type='',subtype='',filtername='',
                 ra=0,dec=0,pointing_angle=0):
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

    def add_options(self,obj):
        for opt in obj:
            self.options.append(opt)
        session.commit()
        return

    @staticmethod            
    def get(dp_id,how='sql'):
        if how=='sql':
            rs = session.query(DataProduct).get(int(dp_id))
        elif how=='pd':
            with engine.connect() as conn:
                rs = pd.read_sql_query(select([DataProduct])
                        .where(DataProduct.dp_id==int(dp_id))
                        ,conn)
                rs = rs.iloc[0]
        return rs    
    
    
class Task(BaseMixin,Base):
    __tablename__= 'tasks'
    
    task_id = Column(Integer, Sequence('task_id_seq'),
                     primary_key=True, nullable=False)
    
    name = Column(postgresql.VARCHAR(64),nullable=False)
    
    pipeline_id = Column(Integer, ForeignKey('pipelines.pipeline_id'))

    masks = relationship('Mask',
                        backref=backref('tasks',
                        uselist=True,passive_updates=False,
                        cascade='delete,all'))
    
    jobs = relationship('Job',
                        backref=backref('tasks',
                        uselist=True,passive_updates=False,
                        cascade='delete,all'))
    
    nruns = Column(Float)
    runtime = Column(Float)
    is_exclusive = Column(Boolean)
    
    timestamp = Column(DateTime, default=func.now())
    
    def __init__(self,name='',
                 nruns=0,run_time=0,
                 is_exclusive=0):
        self.name = str(name)        
        self.nruns = int(nruns)
        self.run_time = float(run_time)
        self.is_exclusive = bool(is_exclusive)
    
    def add_mask(self,obj):
        self.masks.append(obj)
        session.commit()
        return
        
    def add_job(self,obj):
        self.jobs.append(obj)
        session.commit()
        return
    
    @staticmethod            
    def get(task_id,how='sql'):
        if how=='sql':
            rs = session.query(Task).get(int(task_id))
        elif how=='pd':
            with engine.connect() as conn:
                rs = pd.read_sql_query(select([Task])
                        .where(Task.task_id==int(task_id))
                        ,conn)
                rs = rs.iloc[0]
        return rs  
        
class Mask(BaseMixin,Base):
    __tablename__= 'masks'
    
    mask_id = Column(Integer, Sequence('mask_id_seq'),
                     primary_key=True, nullable=False)
    
    task_id = Column(Integer, ForeignKey('tasks.task_id'))
    
    source = Column(postgresql.VARCHAR(64),nullable=False)
    name = Column(postgresql.VARCHAR(64),nullable=False)
    value = Column(postgresql.VARCHAR(64),nullable=False)
    
    timestamp = Column(DateTime, default=func.now())
    
    def __init__(self,source='',name='',value=''):
        self.source = str(source)
        self.name   = str(name)
        self.value  = str(value)
                
    @staticmethod            
    def get(mask_id,how='sql'):
        if how=='sql':
            rs = session.query(Mask).get(int(mask_id))
        elif how=='pd':
            with engine.connect() as conn:
                rs = pd.read_sql_query(select([Mask])
                        .where(Mask.mask_id==int(mask_id))
                        ,conn)
                rs = rs.iloc[0]
        return rs 

    
class Job(BaseMixin,Base):
    __tablename__= 'jobs'
    
    job_id = Column(BigInteger, Sequence('job_id_seq'),
                     primary_key=True, nullable=False)
    
    task_id = Column(Integer, ForeignKey('tasks.task_id'))
    
    config_id = Column(Integer, ForeignKey('configurations.config_id'))
    
    options = relationship('Options',
                           backref=backref('jobs',
                           uselist=True,passive_updates=False,
                           cascade='delete,all'))

    events = relationship('Event', secondary='job_event_link')
    
    nodes = relationship('Node', secondary='job_node_link')
    
    state = Column(postgresql.VARCHAR(64),nullable=False)
    starttime = Column(DateTime, default=func.now())
    endtime = Column(DateTime, default=func.now())
    
    timestamp = Column(DateTime, default=func.now())
    
    
    def __init__(self,state='new'):
        self.state = str(state)
    
    def add_options(self,obj):
        for opt in obj:
            self.options.append(opt)
        session.commit()
        return
 
    def add_event(self,obj):
        self.events.append(obj)
        session.commit()
        return
        
    def add_node(self,obj):
        self.nodes.append(obj)
        session.commit()
        return
    
    @staticmethod            
    def get(job_id,how='sql'):
        if how=='sql':
            rs = session.query(Job).get(int(job_id))
        elif how=='pd':
            with engine.connect() as conn:
                rs = pd.read_sql_query(select([Job])
                        .where(Job.job_id==int(job_id))
                        ,conn)
                rs = rs.iloc[0]
        return rs 
    
class Event(BaseMixin,Base):
    __tablename__= 'events'
    
    event_id = Column(BigInteger, Sequence('event_id_seq'),
                     primary_key=True, nullable=False)
    
    jobs = relationship('Job', secondary='job_event_link')
    
    jargs = Column(postgresql.VARCHAR(64),nullable=False)
    name = Column(postgresql.VARCHAR(64),nullable=False)
    value = Column(postgresql.VARCHAR(64),nullable=False)

    options = relationship('Options',
                           backref=backref('events',
                           uselist=True,passive_updates=False,
                           cascade='delete,all'))
    
    timestamp = Column(DateTime, default=func.now())
    
    def __init__(self,name='any',value='',jargs=''):
        self.jargs  = str(jargs)
        self.name   = str(name)
        self.value  = str(value)
        
    def add_job(self,obj):
        self.jobs.append(obj)
        session.commit()
        return

    @staticmethod            
    def get(event_id,how='sql'):
        if how=='sql':
            rs = session.query(Event).get(int(event_id))
        elif how=='pd':
            with engine.connect() as conn:
                rs = pd.read_sql_query(select([Event])
                        .where(Event.event_id==int(event_id))
                        ,conn)
                rs = rs.iloc[0]
        return rs
      
    
class Node(BaseMixin,Base):
    __tablename__= 'nodes'
    
    node_id = Column(Integer, Sequence('node_id_seq'),
                     primary_key=True, nullable=False)
    name = Column(postgresql.VARCHAR(64),nullable=False)
    
    jobs = relationship('Job', secondary='job_node_link')
    
    int_ip = Column(postgresql.INET)
    ext_ip = Column(postgresql.INET)
    
    timestamp = Column(DateTime, default=func.now())
    
    def __init__(self,name='any',int_ip='',ext_ip=''):
        self.name = str(name)
        self.int_ip = int_ip
        self.ext_ip = ext_ip
        
    @classmethod
    def add_job(self,obj):
        self.jobs.append(obj)
        session.commit()
        return
    
    @staticmethod            
    def get(node_id,how='sql'):
        if how=='sql':
            rs = session.query(Node).get(int(node_id))
        elif how=='pd':
            with engine.connect() as conn:
                rs = pd.read_sql_query(select([Node])
                        .where(Node.node_id==int(node_id))
                        ,conn)
                rs = rs.iloc[0]
        return rs

    
class JobEventLink(Base):
    __tablename__ = 'job_event_link'    
    job_id = Column(Integer, ForeignKey('jobs.job_id'), primary_key=True)
    event_id = Column(Integer, ForeignKey('events.event_id'), primary_key=True)

    
class JobNodeLink(Base):
    __tablename__ = 'job_node_link'    
    job_id = Column(Integer, ForeignKey('jobs.job_id'), primary_key=True)
    node_id = Column(Integer, ForeignKey('nodes.node_id'), primary_key=True)
#
#Base.metadata.create_all(engine)
#
#session.commit()
pass;
