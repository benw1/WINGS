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
    def create(cls):
        session.add(obj)
        session.commit()
        return obj

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
    
    def __init__(self, name, value):
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
        
    def get(user_name):
        with engine.connect() as conn:
            _df = pd.read_sql_query(select([User])
                    .where(User.name==str(user_name))
                    ,conn)
        return _df
    
    @classmethod
    def add_pipeline(cls,obj):
        cls.pipelines.append(obj)
        session.commit()
        return cls
    
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
        
    def get(pipeline_id):
        with engine.connect() as conn:
            _df = pd.read_sql_query(select([Pipeline])
                    .where(Pipeline.pipeline_id==int(pipeline_id))
                    ,conn)
        return _df
    
    @classmethod
    def add_target(cls,obj):
        cls.targets.append(obj)
        session.commit()
        return cls
    
    @classmethod
    def add_task(cls,obj):
        cls.tasks.append(obj)
        session.commit()
        return cls
    
    
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
    
    
    def __init__(self,name,create_dir=False):
        self.name = str(name)
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
        
    @classmethod
    def add_configuration(cls,obj):
        cls.configurations.append(obj)
        session.commit()
        return cls
    
    @classmethod
    def add_options(cls,obj):
        for opt in obj:
            cls.options.append(opt)
        session.commit()
        return cls
    
    @classmethod
    def add_paths(cls,pipeline_id,create_dir=False):
        pipeline = Pipeline.get(int(pipeline_id))
        pipeline = pipeline.iloc[0]
        cls.relativepath = str(pipeline.data_root)+'/'+str(cls.name)
        
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

    def __init__(self,name,description,create_dir=False):
        self.name = str(name)
        self.description = str(description)

    @classmethod
    def add_dp(cls,obj):
        cls.data_products.append(obj)
        session.commit()
        return cls

    @classmethod
    def add_parameters(cls,obj):
        for param in obj:
            cls.parameters.append(param)
        session.commit()
        return cls
    
    @classmethod
    def add_paths(cls,target_id,create_dir=False):
        target = Target.get(int(target_id))
        target = target.iloc[0]
        cls.relativepath = str(target.relativepath)
        cls.logpath = str(target.relativepath)+'/log_'+str(name)
        cls.confpath = str(target.relativepath)+'/conf_'+str(name)
        cls.rawpath = str(target.relativepath)+'/raw_'+str(name)
        cls.procpath = str(target.relativepath)+'/proc_'+str(name)
        
        if create_dir:
            for _path in [cls.rawpath,cls.confpath,cls.procpath,cls.logpath]:
                _t = subprocess.run(['mkdir', '-p', str(_path)], stdout=subprocess.PIPE)        
        
    @classmethod
    def add_job(cls,obj):
        cls.jobs.append(obj)
        session.commit()
        return cls
    
                
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
                        
    name = Column(postgresql.VARCHAR(64),nullable=False)
    value = Column(postgresql.VARCHAR(64),nullable=False)
    
    def __init__(self, name, value):
        self.name = str(name)
        self.value = str(value)
        
    def create(self,param={'any':1}):
        params = []
        for item in param.items():
            params.append(Parameters(item[0],item[1]))
        session.add_all(params)
        session.commit()
        return params
    
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


    def get(dp_id):
        with engine.connect() as conn:
            _df = pd.read_sql_query(select([DataProduct])
                    .where(DataProduct.dp_id==int(dp_id))
                    ,conn)
        return _df
    
    @classmethod
    def add_options(cls,obj):
        for opt in obj:
            cls.options.append(opt)
        session.commit()
        return cls
    
    
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
        
    def get(task_id):
        with engine.connect() as conn:
            _df = pd.read_sql_query(select([Task])
                    .where(Task.task_id==int(task_id))
                    ,conn)
        return _df
    
    @classmethod
    def add_mask(cls,obj):
        cls.masks.append(obj)
        session.commit()
        return cls
        
    @classmethod
    def add_job(cls,obj):
        cls.jobs.append(obj)
        session.commit()
        return cls
    
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
    
    task_id = Column(Integer, ForeignKey('tasks.task_id'))
    
    config_id = Column(Integer, ForeignKey('configurations.config_id'))
    
    options = relationship('Options',
                           backref=backref('jobs',
                           uselist=True,passive_updates=False,
                           cascade='delete,all'))

    events = relationship('Event', backref='jobs', secondary='job_event_link')
    
    nodes = relationship('Node', backref='jobs', secondary='job_node_link')
    
    state = Column(postgresql.VARCHAR(64),nullable=False)
    starttime = Column(DateTime, default=func.now())
    endtime = Column(DateTime, default=func.now())
    
    timestamp = Column(DateTime, default=func.now())
    
    
    def __init__(self,state):
        self.state = str(state)
    
    
    def get(job_id):
        with engine.connect() as conn:
            _df = pd.read_sql_query(select([Job])
                    .where(Job.job_id==int(job_id))
                    ,conn)
        return _df
    
    @classmethod
    def add_options(cls,obj):
        for opt in obj:
            cls.options.append(opt)
        session.commit()
        return cls
 
    @classmethod
    def add_event(cls,obj):
        cls.events.append(obj)
        session.commit()
        return cls
        
    @classmethod
    def add_node(cls,obj):
        cls.nodes.append(obj)
        session.commit()
        return cls
    
class Event(BaseMixin,Base):
    __tablename__= 'events'
    
    event_id = Column(BigInteger, Sequence('event_id_seq'),
                     primary_key=True, nullable=False)
    
    jobs = relationship('Job', backref='events')
    
    jargs = Column(postgresql.VARCHAR(64),nullable=False)
    name = Column(postgresql.VARCHAR(64),nullable=False)
    value = Column(postgresql.VARCHAR(64),nullable=False)

    options = relationship('Options',
                           backref=backref('events',
                           uselist=True,passive_updates=False,
                           cascade='delete,all'))
    
    timestamp = Column(DateTime, default=func.now())
    
    def __init__(self,name,value,jargs):
        self.jargs  = str(jargs)
        self.name   = str(name)
        self.value  = str(value)
        
    def get(event_id):
        with engine.connect() as conn:
            _df = pd.read_sql_query(select([Event])
                    .where(Event.event_id==int(event_id))
                    ,conn)
        return _df

    @classmethod
    def add_job(cls,obj):
        cls.jobs.append(obj)
        session.commit()
        return cls

    
    
class Node(BaseMixin,Base):
    __tablename__= 'nodes'
    
    node_id = Column(Integer, Sequence('node_id_seq'),
                     primary_key=True, nullable=False)
    name = Column(postgresql.VARCHAR(64),nullable=False)
    
    jobs = relationship('Job', backref='nodes')
    
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
    
    @classmethod
    def add_job(cls,obj):
        cls.jobs.append(obj)
        session.commit()
        return cls
    
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
