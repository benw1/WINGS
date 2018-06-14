#! /usr/bin/env
import time, subprocess, os
import numpy as np
import pandas as pd
pd.set_option('io.hdf.default_format','table')

path_to_store='./h5data/wpipe_store.h5'

path_to_store=os.path.abspath(path_to_store)

def update_time(x):
    x.timestamp = pd.to_datetime(time.time(),unit='s')
    return x        

def _min_itemsize(x):
    min_itemsize = {}
    for k,_dt in dict(x.dtypes).items():
        if _dt is np.dtype('O'):
            min_itemsize[k] = int(256)
    return min_itemsize

class Store():
    def __init__(self,storePath=path_to_store):
        self.path = str(storePath)
        return None
    
    def new(self):
        _dict = {'users': User().new(),
                 'nodes': Node().new(),
                 'options': Options().new(),
                 'pipelines': Pipeline().new(),
                 'targets': Target().new(),
                 'configurations': Configuration().new(),
                 'data_products': DataProduct().new(),
                 'parameters': Parameters().new(),
                 'tasks': Task().new(),
                 'jobs': Job().new(),
                 'masks':  Mask().new(),
                 'events': Event().new()}
        with pd.HDFStore(str(self.path),'w',complevel=9,
                    complib='blosc:blosclz') as myStore:
            for k,v in _dict.items():
                myStore.append(k,v,min_itemsize=_min_itemsize(v),
                          complevel=9,complib='blosc:blosclz')
        return None

    def create(self,key,name_id,stuff):
        with pd.HDFStore(str(self.path),'r+') as myStore:
            stuff.__dict__[name_id][0] = int(myStore[key][name_id].max())+1
            newStuff = stuff.new()
            myStore.append(key,newStuff,min_itemsize=_min_itemsize(newStuff),
                          complevel=9,complib='blosc:blosclz')
        return newStuff
        
    
    def select(self,key='events',where='all',columns=None):
        with pd.HDFStore(str(self.path),'r') as myStore:
            if where=='all':
                return myStore.select(str(key),columns=columns)
            else:
                return myStore.select(str(key),columns=columns).query(str(where))
            
    def repack(self):
        filename = str(self.path)
        _t1 = ['cp', filename, './data/backup1.h5']
        _t2 = ['ptrepack', '--chunkshape=auto', '--propindexes', '--complevel=9',
               '--complib=blosc', filename, './data/temp1.h5']
        _t3 = ['mv', './data/temp1.h5', filename]
        _t = subprocess.run(_t1, stdout=subprocess.PIPE)
        _t = subprocess.run(_t2, stdout=subprocess.PIPE)
        _t = subprocess.run(_t3, stdout=subprocess.PIPE)
        return
                

class User():
    def __init__(self,name='any'):
        self.name = np.array([str(name)],dtype='<U20')
        self.user_id = np.array([int(0)])
        self.timestamp = pd.to_datetime(time.time(),unit='s')
        return None

    def new(self):
        _df = pd.DataFrame.from_dict(self.__dict__)
        _df.index = _df.user_id
        return update_time(_df)

    def create(self,store=Store()):
        return store.create('users','user_id',self)

    
class Node():
    def __init__(self,name='any',int_ip='',ext_ip=''):
        self.name = np.array([str(name)])
        self.node_id = np.array([int(0)])
        self.int_ip = np.array([str(int_ip)])
        self.ext_ip = np.array([str(ext_ip)])
        self.timestamp = pd.to_datetime(time.time(),unit='s')
        return None
    
    def new(self):
        _df = pd.DataFrame.from_dict(self.__dict__)
        _df.index = _df.node_id
        return update_time(_df)
    
    def create(self,store=Store()):
        return store.create('nodes','node_id',self)

    
class Options():
    def __init__(self,opts={'any':0}):
        self.__dict__ = opts
        return None

    def new(self,owner=str('any'+250*' '),owner_id=0):
        name = np.array(list(self.__dict__.keys()))
        value = np.array(list(self.__dict__.values()))
        _df = pd.DataFrame(data=np.array([name,value]).T,columns=
                ['name','value']).sort_values('name')
        owner = np.repeat(str(owner),len(name))
        owner_id = np.repeat(int(owner_id),len(name))
        arrays = [owner,owner_id]
        _df.index = pd.MultiIndex.from_arrays(arrays, names=('owner','owner_id'))
        return _df
    
    def create(self,owner='any',owner_id=0,store=Store()):
        _df = self.new(owner,owner_id)
        with pd.HDFStore(str(store.path),'r+') as myStore:
            myStore.append('options',_df,min_itemsize=_min_itemsize(_df),
                          complevel=9,complib='blosc:blosclz')
        return _df

'''
    #needs work
    def add_or_update(opt1,opt2,store=Store()):
        owner,owner_id = opt2.index[0]
        dict1 = dict(zip(opt1.values[:,0],opt1.values[:,1]))
        dict2 = dict(zip(opt2.values[:,0],opt2.values[:,1]))
        for key in list(dict2.keys()):
            dict1[key] = dict2[key]
        return Options(dict1).create(owner,owner_id)
        
    # needs work
    def get_opt(opt,owner='any',owner_id=0,store=Store()):
        
        _array = opt.sort_index(level=['owner','owner_id']).loc[str(owner),int(owner_id)].values
        return dict(zip(_array[:,0],_array[:,1]))
'''
         
class Pipeline():
    def __init__(self,user=User().new(),name='any',software_root='',
                 data_root='',pipe_root='',config_root='',
                 description=''):
        self.name = np.array([str(name)])
        self.user_name = np.array([str(user.name)])           
        self.user_id = np.array([int(user.user_id)])
        self.pipeline_id = np.array([int(0)])
        self.software_root = np.array([str(software_root)])
        self.data_root = np.array([str(data_root)])
        self.pipe_root = np.array([str(pipe_root)])
        self.config_root = np.array([str(config_root)])
        self.description = np.array([str(description)])
        self.timestamp = pd.to_datetime(time.time(),unit='s')
        return None
                                 
    def new(self):
        _df = pd.DataFrame.from_dict(self.__dict__)
        _df.index = _df.pipeline_id
        return update_time(_df)

    def create(self,store=Store()):
        _df = store.create('pipelines','pipeline_id',self)       
        return _df

        
class Target():
    def __init__(self,name='any',relativepath='',
                 pipeline=Pipeline().new()):               
        self.name = np.array([str(name)])
        self.relativepath = np.array([str(relativepath)])
        self.pipeline_id = np.array([int(pipeline.pipeline_id)])
        self.target_id = np.array([int(0)])
        self.timestamp = pd.to_datetime(time.time(),unit='s')
        return None

    def new(self):
        _df = pd.DataFrame.from_dict(self.__dict__)
        _df.index = pd.MultiIndex.from_arrays(arrays=[np.array([int(self.pipeline_id)]),
                    np.array([int(self.target_id)])], names=('pipelineID','targetID'))
        return update_time(_df) 

    def create(self,options={'any':0},ret_opt=True,store=Store()):
        _df = store.create('targets','target_id',self)
        _opt = Options(options).create('target',int(_df.target_id))
        if ret_opt:
            return _df, _opt
        else:
            return _df        


class Configuration():
    def __init__(self,name='',relativepath='',description='',
                 target=Target().new()):
        self.name = np.array([str(name)])
        self.relativepath = np.array([str(relativepath)])
        self.target_id = np.array([int(target.target_id)])
        self.pipeline_id = np.array([int(target.pipeline_id)])
        self.config_id = np.array([int(0)])
        self.description = np.array([str(description)])
        self.timestamp = pd.to_datetime(time.time(),unit='s')
        return None
        
    def new(self):
        _df = pd.DataFrame.from_dict(self.__dict__)
        _df.index = pd.MultiIndex.from_arrays(arrays=[np.array([int(self.pipeline_id)]),
                    np.array([int(self.target_id)]), np.array([int(self.config_id)])],
                    names=('pipelineID','targetID','configID'))
        return update_time(_df)        

        
    def create(self,options={'any':0},ret_opt=True,store=Store()):
        _df = store.create('configurations','config_id',self)
        _opt = Options(options).create('config',int(self.config_id))
        if ret_opt:
            return _df, _opt
        else:
            return _df    
        
class DataProduct():
    def __init__(self,filename='any',relativepath='',group='',
                 configuration=Configuration().new(),
                 data_type='',subtype='',filtername='',
                 ra=0,dec=0,pointing_angle=0):
        self.config_id = np.array([int(configuration.config_id)])
        self.target_id = np.array([int(configuration.target_id)])
        self.pipeline_id = np.array([int(configuration.pipeline_id)])
        self.dp_id = np.array([int(0)])

        self.filename = np.array([str(filename)])
        self.relativepath = np.array([str(relativepath)])

        _suffix = ' '
        if '.' in filename:
            _suffix = filename.split('.')[-1]
        if _suffix not in ['fits','txt','head','cl',
           'py','pyc','pl','phot','png','jpg','ps',
           'gz','dat','lst','sh']:
            _suffix = 'other'                                      
        self.suffix = np.array([str(_suffix)])
        
        if not(data_type): data_type = _suffix
        self.data_type = np.array([str(data_type)])
        self.subtype = np.array([str(subtype)])

        if group not in ['proc','conf','log','raw']:
            group = 'other'
        self.group = np.array([str('other')])

        self.filtername = np.array([str(filtername)])
        self.ra = np.array([float(ra)])
        self.dec = np.array([float(dec)])
        self.pointing_angle = np.array([float(pointing_angle)])
        # self.tags = Options(tags) # meant to break
        self.timestamp = pd.to_datetime(time.time(),unit='s')
        return None

    def new(self):        
        _df = pd.DataFrame.from_dict(self.__dict__)
        _df.index = pd.MultiIndex.from_arrays(arrays=[np.array([int(self.pipeline_id)]),
                    np.array([int(self.target_id)]), np.array([int(self.config_id)]),
                    np.array([int(self.dp_id)])],
                    names=('pipelineID','targetID','configID','dpID'))
        return update_time(_df)

    def create(self,options={'any':0},ret_opt=True,store=Store()):
        _df = store.create('data_products','dp_id',self)
        _opt = Options(options).create('data_product',int(self.dp_id))
        if ret_opt:
            return _df, _opt
        else:
            return _df    
        
class Parameters():
    def __init__(self,params={'any':0}):
        self.__dict__ = params
        return None
 
    def new(self,config=Configuration().new()):
        name = np.array(list(self.__dict__.keys()))
        value = np.array(list(self.__dict__.values()))
        _df = pd.DataFrame(data=np.array([name,value]).T,columns=
                ['name','value']).sort_values('name')
        
        _config_id = np.repeat(int(config.config_id),len(name))
        _target_id = np.repeat(int(config.target_id),len(name))
        _pipeline_id = np.repeat(int(config.pipeline_id),len(name))
        arrays = [_pipeline_id,_target_id,_config_id]
        _df.index= pd.MultiIndex.from_arrays(arrays,
                     names=('pipelineID','targetID','configID'))
        return _df

    def create(self,config=Configuration().new(),store=Store()):
        _df = self.new(config)
        with pd.HDFStore(str(store.path),'r+') as myStore:
            myStore.append('parameters',_df,min_itemsize=_min_itemsize(_df))
        return _df

'''    
    # needs work
    def get_params(cls,config=Configuration().new()):
        _config_id = int(config.config_id)
        _target_id = int(config.target_id)
        _pipeline_id = int(config.pipeline_id)
        _array = cls.loc[_pipeline_id,_target_id,_config_id].values
        return dict(zip(_array[:,0],_array[:,1]))
'''    

class Task():
    def __init__(self,name='any',flags='',
                 pipeline=Pipeline().new(),
                 nruns=0,run_time=0,
                 is_exclusive=0):
        self.name = np.array([str(name)])
        self.pipeline_id = np.array([int(pipeline.pipeline_id)])
        self.task_id = np.array([int(0)])
        self.nruns = np.array([int(nruns)])
        self.run_time = np.array([float(run_time)])
        self.is_exclusive = np.array([bool(is_exclusive)])
        # self.mask = mask
        self.timestamp = pd.to_datetime(time.time(),unit='s')
        return None

    def new(self):        
        _df = pd.DataFrame.from_dict(self.__dict__)
        _df.index = pd.MultiIndex.from_arrays(arrays=[np.array([int(self.pipeline_id)]),
                    np.array([int(self.task_id)])], names=('pipelineID','taskID'))
        return update_time(_df)

    def create(self,store=Store()):
        _df = store.create('tasks','task_id',self)
        return _df    
        
    def add_mask(task,source='any',name='any',value='0'):
        return Mask(task,source,name,value).create()
        
        
class Job():
    def __init__(self,state='any',event_id=0,
                 task=Task().new(),
                 config=Configuration().new(),
                 node=Node().new()):
        self.state = np.array([str(state)])
        self.job_id = np.array([int(0)])
        self.event_id = np.array([int(event_id)])
        self.task_id = np.array([int(task.task_id)])
        self.config_id = np.array([int(config.config_id)])
        self.node_id =  np.array([int(node.node_id)])
        self.pipeline_id =  np.array([int(config.pipeline_id)])
        self.starttime = pd.to_datetime(time.time(),unit='s')
        self.endtime = pd.to_datetime(time.time(),unit='s')
        self.timestamp = pd.to_datetime(time.time(),unit='s')
        return None
    
    def new(self):
        _df = pd.DataFrame.from_dict(self.__dict__)
        _df.index = update_time((pd.MultiIndex.from_arrays(arrays=
                   [np.array([int(self.pipeline_id)]),
                    np.array([int(self.task_id)]),
                    np.array([int(self.config_id)]),
                    np.array([int(self.event_id)]),
                    np.array([int(self.job_id)])],
                    names=('pipelineID','taskID','configID','eventID','jobID'))))
        _df.endtime = _df.timestamp.copy()
        return _df
        
    def create(self,options={'any':0},ret_opt=True,store=Store()):
        _df = store.create('jobs','job_id',self)
        _opt = Options(options).create('job',int(self.job_id))
        if ret_opt:
            return _df, _opt
        else:
            return _df   
        
class Event():
    def __init__(self,job=Job().new(),jargs='',name='',value=''):
        ''' source' has to be an existing task_name for this pipeline or wildcard '''
        self.job_id = np.array([int(job.job_id)])
        self.jargs = np.array([str(jargs)])
        self.name   = np.array([str(name)])
        self.value  = np.array([str(value)])
        self.event_id  = np.array([int(0)])
        return None
    
    def new(self):
        _df = pd.DataFrame.from_dict(self.__dict__)
        _df.index = _df.event_id
        return update_time(_df)
    
    def create(self,store=Store()):
        return store.create('events','event_id',self)
        

class Mask():
    def __init__(self,task=Task().new(),source='',name='',value=''):
        ''' source' has to be an existing task_name for this pipeline or wildcard '''
        self.source = np.array([str(source)])
        self.name   = np.array([str(name)])
        self.value  = np.array([str(value)])
        self.task_id = np.array([int(task.task_id)])
        self.mask_id = np.array([int(0)])
        return None

    def new(self):
        _df = pd.DataFrame.from_dict(self.__dict__)
        _df.index = _df.mask_id
        return update_time(_df)
    
    def create(self,store=Store()):
        return store.create('masks','mask_id',self)   
        
