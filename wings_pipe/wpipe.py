#! /usr/bin/env python
import time
import pandas as pd
import numpy as np

def update_time(x):
    x.timestamp = pd.to_datetime(time.time(),unit='s')
    return x

class User():
    def __init__(self,name='any',user_id=0):
        self.name = np.array([str(name)])
        self.user_id = np.array([int(user_id)])
        self.timestamp = pd.to_datetime(time.time(),unit='s')
        return None

    def create(self):
        return update_time(pd.DataFrame(data=self.__dict__,columns=
                ['user_id','name','timestamp']))

class Node():
    def __init__(self,name='any',node_id=0,int_ip='',ext_ip=''):
        self.name = np.array([str(name)])
        self.node_id = np.array([int(node_id)])
        self.int_ip = np.array([str(int_ip)])
        self.ext_ip = np.array([str(ext_ip)])
        self.timestamp = pd.to_datetime(time.time(),unit='s')
        return None
    
    def create(self):
        return update_time(pd.DataFrame(data=self.__dict__,columns=
                ['name','node_id','int_ip','ext_ip','timestamp']))
    
class Options():
    def __init__(self,opts={'any':0}):
        self.__dict__ = opts
        return None

    def create(self,owner='any',owner_id=0):
        name = np.array(list(self.__dict__.keys()))
        value = np.array(list(self.__dict__.values()))
        _df = pd.DataFrame(data=np.array([name,value]).T,columns=
                ['name','value']).sort_values('name')
        owner = np.repeat(str(owner),len(name))
        owner_id = np.repeat(int(owner_id),len(name))
        arrays = [owner,owner_id]
        _df.index = pd.MultiIndex.from_arrays(arrays, names=('owner','owner_id'))
        return _df
    
    def add_or_update(opt1,opt2):
        owner,owner_id = opt2.index[0]
        dict1 = dict(zip(opt1.values[:,0],opt1.values[:,1]))
        dict2 = dict(zip(opt2.values[:,0],opt2.values[:,1]))
        for key in list(dict2.keys()):
            dict1[key] = dict2[key]
        return Options(dict1).create(owner,owner_id)
        
    def get_opt(opt,owner='any',owner_id=0):
        _array = opt.sort_index(level=['owner','owner_id']).loc[str(owner),int(owner_id)].values
        return dict(zip(_array[:,0],_array[:,1]))

            
class Pipeline():
    def __init__(self,user=User().create(),name='any',pipeline_id=0,software_root='',
                 data_root='',pipe_root='',config_root='',
                 description='',state_id=0):
        self.name = np.array([str(name)])
        self.user_name = np.array([str(user.name[0])])           
        self.user_id = np.array([int(user.user_id)])
        self.pipeline_id = np.array([int(pipeline_id)])
        self.software_root = np.array([str(software_root)])
        self.data_root = np.array([str(data_root)])
        self.pipe_root = np.array([str(pipe_root)])
        self.config_root = np.array([str(config_root)])                                                                                                                                                             
        self.state_id = np.array([int(state_id)])
        self.description = np.array([str(description)])
        self.timestamp = pd.to_datetime(time.time(),unit='s')
        return None
                                 
    def create(self,options={'any':0},ret_opt=True):
        _opt = Options(options).create('pipeline',int(self.pipeline_id))
        _df = pd.DataFrame(data=self.__dict__,columns=['user_id','user_name',
                 'name','pipeline_id','software_root=',
                 'data_root','pipe_root','config_root=',
                 'description','state_id','timestamp'])
        _df.index = pd.MultiIndex.from_arrays(arrays=[np.array([str(self.user_name[0])]),
                    np.array([int(self.pipeline_id)])], names=('user_name','pipeline_id'))
        if ret_opt:
            return update_time(_df), _opt
        else:
            return update_time(_df)

class Target():
    def __init__(self,name='any',target_id=0,relativepath='',
                 pipeline=Pipeline().create(ret_opt=False)):               
        self.name = np.array([str(name)])
        self.relativepath = np.array([str(relativepath)])
        self.pipeline_id = np.array([int(pipeline.pipeline_id)])
        self.target_id = np.array([int(target_id)])
        self.timestamp = pd.to_datetime(time.time(),unit='s')
        return None

    def create(self,options={'any':0},ret_opt=True):
        _opt = Options(options).create('target',int(self.target_id))
        _df = pd.DataFrame(data=self.__dict__,columns=
                ['name','relativepath','pipeline_id','target_id','timestamp'])
        _df.index = pd.MultiIndex.from_arrays(arrays=[np.array([int(self.pipeline_id)]),
                    np.array([int(self.target_id)])], names=('pipeline_id','target_id'))
        if ret_opt:
            return update_time(_df), _opt
        else:
            return update_time(_df)        


class Configuration():
    def __init__(self,name='',relativepath='',description='',
                 target=Target().create(ret_opt=False),
                 config_id =1):
        self.name = np.array([str(name)])
        self.relativepath = np.array([str(relativepath)])
        self.target_id = np.array([int(target.target_id)])
        self.pipeline_id = np.array([int(target.pipeline_id)])
        self.config_id = np.array([int(config_id)])
        self.description = np.array([str(description)])
        self.timestamp = pd.to_datetime(time.time(),unit='s')
        return None
        
    def create(self,options={'any':0},ret_opt=True):
        _opt = Options(options).create('config',int(self.config_id))
        _df = pd.DataFrame(data=self.__dict__,columns=
                ['name','relativepath','pipeline_id','target_id',
                'config_id','description','timestamp'])
        _df.index = pd.MultiIndex.from_arrays(arrays=[np.array([int(self.pipeline_id)]),
                    np.array([int(self.target_id)]), np.array([int(self.config_id)])],
                    names=('pipeline_id','target_id','config_id'))
        if ret_opt:
            return update_time(_df), _opt
        else:
            return update_time(_df)        

class DataProduct():
    def __init__(self,filename='any',relativepath='',group='',
                 configuration=Configuration().create(ret_opt=False),
                 dp_id=0,data_type='',subtype='',filtername='',
                 ra=0,dec=0,pointing_angle=0,**tags):
        self.config_id = np.array([int(configuration.config_id)])
        self.target_id = np.array([int(configuration.target_id)])
        self.pipeline_id = np.array([int(configuration.pipeline_id)])
        self.dp_id = np.array([int(dp_id)])

        self.filename = np.array([str(filename)])
        self.relativepath = np.array([str(relativepath)])

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

    def create(self,options={'any':0},ret_opt=True):
        _opt = Options(options).create('data_product',int(self.dp_id))
        _df = pd.DataFrame(data=self.__dict__,columns=
                ['filename','relativepath','pipeline_id','target_id',
                'config_id','dp_id','suffix','data_type',
                'subtype','group','filtername','ra','dec',
                 'pointing_angle','timestamp'])
        _df.index = pd.MultiIndex.from_arrays(arrays=[np.array([int(self.pipeline_id)]),
                    np.array([int(self.target_id)]), np.array([int(self.config_id)]),
                    np.array([int(self.dp_id)])],
                    names=('pipeline_id','target_id','config_id','dp_id'))
        if ret_opt:
            return update_time(_df), _opt
        else:
            return update_time(_df)


class Parameters():
    def __init__(self,params={'any':0}):
        self.__dict__ = params
        return None
 
    def create(self,config=Configuration().create(ret_opt=False),ret_opt=True):
        name = np.array(list(self.__dict__.keys()))
        value = np.array(list(self.__dict__.values()))
        _df = pd.DataFrame(data=np.array([name,value]).T,columns=
                ['name','value'])
        
        _config_id = np.repeat(int(config.config_id),len(name))
        _target_id = np.repeat(int(config.target_id),len(name))
        _pipeline_id = np.repeat(int(config.pipeline_id),len(name))
        arrays = [_pipeline_id,_target_id,_config_id]
        _df.index= pd.MultiIndex.from_arrays(arrays,
                     names=('pipeline_id','target_id','config_id'))
        if ret_opt:
            return update_time(_df), _opt
        else:
            return update_time(_df)
        
    def get_params(cls,config=Configuration().create(ret_opt=False)):
        _config_id = int(config.config_id)
        _target_id = int(config.target_id)
        _pipeline_id = int(config.pipeline_id)
        _array = cls.loc[_pipeline_id,_target_id,_config_id].values
        return dict(zip(_array[:,0],_array[:,1]))
        
class Task():
    def __init__(self,name='any',task_id=0,flags='',
                 pipeline=Pipeline().create(ret_opt=False),
                 nruns=0,run_time=0,
                 is_exclusive=0):
        self.name = np.array([str(name)])
        self.pipeline_id = np.array([int(pipeline.pipeline_id)])
        self.task_id = np.array([int(task_id)])
        self.nruns = np.array([int(nruns)])
        self.run_time = np.array([float(run_time)])
        self.is_exclusive = np.array([bool(is_exclusive)])
        # self.mask = mask
        self.timestamp = pd.to_datetime(time.time(),unit='s')
        return None

    def create(self,options={'any':0},ret_opt=True):
        _opt = Options(options).create('task',int(self.task_id))
        _df = pd.DataFrame(data=self.__dict__,columns=
                ['name','pipeline_id','task_id','nruns','run_time','is_exclusive','timestamp'])
        _df.index = pd.MultiIndex.from_arrays(arrays=[np.array([int(self.pipeline_id)]),
                    np.array([int(self.task_id)])], names=('pipeline_id','task_id'))
        if ret_opt:
            return update_time(_df), _opt
        else:
            return update_time(_df)
        
    def add_mask(task,source='any',name='any',value='0'):
        return Mask(task,source,name,value).create()
        
        
class Job():
    def __init__(self,state='any',event_id=0,job_id=0,
                 task=Task().create(ret_opt=False),
                 config=Configuration().create(ret_opt=False),
                 node=Node().create()):
        self.state = np.array([str(state)])
        self.job_id = np.array([int(job_id)])
        self.event_id = np.array([int(event_id)])
        self.task_id = np.array([int(task.task_id)])
        self.config_id = np.array([int(config.config_id)])
        self.node_id =  np.array([int(node.node_id)])
        self.pipeline_id =  np.array([int(config.pipeline_id)])
        self.starttime = pd.to_datetime(time.time(),unit='s')
        self.endtime = pd.to_datetime(time.time(),unit='s')
        self.timestamp = pd.to_datetime(time.time(),unit='s')
        return None
    
    def create(self,options={'any':0},ret_opt=True):
        _opt = Options(options).create('job',int(self.job_id))
        _df = pd.DataFrame(data=self.__dict__,columns=
                ['state','event_id','pipeline_id','task_id',
                'config_id','node_id','job_id','starttime',
                 'endtime','timestamp'])
        _df.index = update_time((pd.MultiIndex.from_arrays(arrays=
                   [np.array([int(self.pipeline_id)]),
                    np.array([int(self.task_id)]),
                    np.array([int(self.config_id)]),
                    np.array([int(self.event_id)]),
                    np.array([int(self.job_id)])],
                    names=('pipeline_id','task_id','config_id','event_id','job_id'))))
        _df.endtime = _df.timestamp.copy()
        if ret_opt:
            return _df, _opt
        else:
            return _df        
        
class Event():
    def __init__(self,job=Job().create(ret_opt=False),jargs='',name='',value=''):
        ''' source' has to be an existing task_name for this pipeline or wildcard '''
        self.job_id = np.array([int(job.job_id)])
        self.jargs = np.array([str(jargs)])
        self.name   = np.array([str(name)])
        self.value  = np.array([str(value)])
        return None
    
    def create(self):
        return pd.DataFrame(data=self.__dict__,columns=
                ['job_id','source','name','value'])
        

class Mask():
    def __init__(self,task=Task().create(ret_opt=False),source='',name='',value=''):
        ''' source' has to be an existing task_name for this pipeline or wildcard '''
        self.source = np.array([str(source)])
        self.name   = np.array([str(name)])
        self.value  = np.array([str(value)])
        self.task_id = np.array([int(task.task_id)])
        return None
    
    def create(self):
        return pd.DataFrame(data=self.__dict__,columns=
                ['task_id','source','name','value'])
    

