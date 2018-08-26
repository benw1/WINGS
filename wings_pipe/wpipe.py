#! /usr/bin/env python
import time, subprocess, os
import numpy as np
import pandas as pd
pd.set_option('io.hdf.default_format','table')

path_to_store='/Users/ben/src/WINGS/wings_pipe/h5data/wpipe_store.h5'
# path_to_store='/Users/rubab/Work/WINGS/wings_pipe/h5data/wpipe_store.h5'

def update_time(x):
    x.timestamp = pd.to_datetime(time.time(),unit='s')
    return x

def increment(df,x):
    df[x] = int(df[x])+1
    return df

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
        
    def update(self,key,stuff):
        with pd.HDFStore(str(self.path),'r+') as myStore:
            _t = myStore[key]
            _t = _t.drop(index=stuff.index).append(stuff)
            myStore.remove(key)
            myStore.append(key,_t,min_itemsize=_min_itemsize(_t),
                        complevel=9,complib='blosc:blosclz')
        return None
    
    def select(self,key='events',where='all',columns=None):
        with pd.HDFStore(str(self.path),'r') as myStore:
            if where=='all':
                if columns is None:
                    return myStore.get(str(key))
                else:
                    return myStore.select(str(key),columns=columns)
            else:
                return myStore.select(str(key),columns=columns).query(str(where))
            
    def repack(self):
        filename = str(self.path)
        _t1 = ['cp', filename, './backup1.h5']
        _t2 = ['ptrepack', '--chunkshape=auto', '--propindexes', '--complevel=9',
               '--complib=blosc', filename, './temp1.h5']
        _t3 = ['mv', './temp1.h5', filename]
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

    def get(user_name,store=Store()):
        x = store.select('users','name=="'+str(user_name)+'"')
        return x.loc[x.index.values[0]]
    
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

    def get(node_id,store=Store()):
        return store.select('nodes').loc[int(node_id)]
    
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

    def get(owner,owner_id,store=Store()):
        x = store.select('options').loc[str(owner)].loc[int(owner_id)]
        if x.shape==(2,):
            return dict(zip([x['name']],[x['value']]))
        else:
            return dict(zip(x['name'].values,x['value'].values))
    
    def addOption(owner,owner_id,key,value,store=Store()):
        _opt = Options.get(owner,int(owner_id))
        _opt[key] = value
        return store.update('options',Options(_opt).new(owner,int(owner_id)))

    
class Pipeline():
    def __init__(self,user=User().new(),name='any',software_root='',
                 data_root='',pipe_root='',config_root='',
                 description=''):
        self.name = np.array([str(name)])
        self.user_name = np.array([str(user['name'])])           
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

    def get(pipeline_id,store=Store()):
        return store.select('pipelines').loc[int(pipeline_id)]    
        
class Target():
    def __init__(self,name='any',
                 pipeline=Pipeline().new()):               
        self.name = np.array([str(name)])
        self.pipeline_id = np.array([int(pipeline.pipeline_id)])
        self.target_id = np.array([int(0)])
        myPipe = Pipeline.get(self.pipeline_id)
        self.relativepath = np.array([str(myPipe.data_root)+'/'+str(self.name[0])])
        self.timestamp = pd.to_datetime(time.time(),unit='s')
        return None

    def new(self):
        _df = pd.DataFrame.from_dict(self.__dict__)
        _df.index = pd.MultiIndex.from_arrays(arrays=[np.array([int(self.pipeline_id)]),
                    np.array([int(self.target_id)])], names=('pipelineID','targetID'))
        return update_time(_df) 

    def create(self,options={'any':0},ret_opt=False,create_dir=False,store=Store()):
        _df = store.create('targets','target_id',self)
        _opt = Options(options).create('target',int(_df.target_id),store=store)
        
        if create_dir:
            _t = subprocess.run(['mkdir', '-p', str(self.relativepath)], stdout=subprocess.PIPE)
        
        if ret_opt:
            return _df, _opt
        else:
            return _df       
    
    def get(target_id,store=Store()):
        x = store.select('targets', 'target_id=='+str(target_id))
        return x.loc[x.index.values[0]]


class Configuration():
    def __init__(self,name='',description='',
                 target=Target().new()):
        self.name = np.array([str(name)])
        self.relativepath = np.array([str(target.relativepath[0])])
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

        
    def create(self,params={'any':0},create_dir=False,ret_opt=False,store=Store()):
        _df = store.create('configurations','config_id',self)
        _params = Parameters(params).create(_df,store=store)
        
        if create_dir:
            _t1 = ['mkdir', '-p', str(self.relativepath[0])+'/raw_'+str(self.name[0])]
            _t2 = ['mkdir', '-p', str(self.relativepath[0])+'/conf_'+str(self.name[0])]
            _t3 = ['mkdir', '-p', str(self.relativepath[0])+'/proc_'+str(self.name[0])]
            _t4 = ['mkdir', '-p', str(self.relativepath[0])+'/log_'+str(self.name[0])]
            _t = subprocess.run(_t1, stdout=subprocess.PIPE)
            _t = subprocess.run(_t2, stdout=subprocess.PIPE)
            _t = subprocess.run(_t3, stdout=subprocess.PIPE)
            _t = subprocess.run(_t4, stdout=subprocess.PIPE)
        
        if ret_opt:
            return _df, _params
        else:
            return _df    
        
    def get(config_id,store=Store()):
        x = store.select('configurations', 'config_id=='+str(config_id)) 
        return x.loc[x.index.values[0]]
    
    
    
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

    def create(self,options={'any':0},ret_opt=False,store=Store()):
        _df = store.create('data_products','dp_id',self)
        _opt = Options(options).create('data_product',int(_df.dp_id),store=store)
        if ret_opt:
            return _df, _opt
        else:
            return _df  
        
    def get(dp_id,store=Store()):
        x = store.select('data_products', 'dp_id=='+str(dp_id)) 
        return x.loc[x.index.values[0]]
    
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

    def getParam(config_id=0,store=Store()):
        config_id = int(config_id)
        config = Configuration.get(int(config_id))
        target_id = int(config.target_id)
        pipeline_id = int(config.pipeline_id)
        x = store.select('parameters').loc[pipeline_id,target_id,config_id] 
        if x.shape==(2,):
            return dict(zip([x['name']],[x['value']]))
        else:
            return dict(zip(x['name'].values,x['value'].values))
    
    def addParam(config_id,key,value,store=Store()):
        config_id = int(config_id)
        _config = Configuration.get(config_id)
        _params = Parameters.getParam(config_id)
        _params[key] = value
        return store.update('parameters',Parameters(_params).new(_config))
    
    
class Task():
    def __init__(self,name='any',
                 pipeline=Pipeline().new(),
                 nruns=0,run_time=0,
                 is_exclusive=0):
        self.name = np.array([str(name)])
        self.pipeline_id = np.array([int(pipeline.pipeline_id)])
        self.task_id = np.array([int(0)])
        self.nruns = np.array([int(nruns)])
        self.run_time = np.array([float(run_time)])
        self.is_exclusive = np.array([bool(is_exclusive)])
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
        
    def add_mask(task,source='any',name='any',value='0',store=Store()):
        return Mask(task,source,name,value).create(store=store)
        
    def get(task_id,store=Store()):
        x = store.select('tasks', 'task_id=='+str(task_id))
        return x.loc[x.index.values[0]]    
        
        
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
        
    def create(self,options={'completed':0},ret_opt=False,store=Store()):
        _df = store.create('jobs','job_id',self)
        _opt = Options(options).create('job',int(_df.job_id),store=store)
        if ret_opt:
            return _df, _opt
        else:
            return _df   
        
    def get(job_id,store=Store()):
        x = store.select('jobs', 'job_id=='+str(job_id)) 
        return x.loc[x.index.values[0]]    

    def getEvent(job,
                  name='any',value='0',jargs='0',
                  options={'any':0},store=Store()):
        return Event(name,value,jargs,job).create(options=options,store=store)
    
class Event():
    def __init__(self,name='',value='',jargs='',job=Job().new()):
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
    
    def create(self,options={'any':0},ret_opt=False,store=Store()):
        _df = store.create('events','event_id',self)
        _opt = Options(options).create('event',int(_df.event_id),store=store)
        if ret_opt:
            return _df, _opt
        else:
            return _df   
        
    def get(event_id,store=Store()):
        return store.select('events').loc[int(event_id)]
    
    def run_complete(event_id=0,store=Store()):
        event = Event.get(int(event_id))
        job_id = int(event.job_id)
        jobOpt = Options.get('job',int(job_id))
        jobOpt['completed'] = int(jobOpt['completed'])+1
        return store.update('options',Options(jobOpt).new('job',job_id))

class Mask():
    def __init__(self,task=Task().new(),source='',name='',value=''):
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
    
    def get(mask_id,store=Store()):
        return store.select('masks').loc[int(mask_id)]

def Submit(task,job_id,event_id):
    pid = task.pipeline_id
    myPipe = Pipeline.get(pid)
    swroot = myPipe.software_root
    executable = swroot+'/'+task['name']
    dataroot = myPipe.data_root
    job = Job.get(int(job_id))
    #subprocess.Popen([executable,'-e',str(event_id),'-j',str(job_id)],cwd=dataroot) # This line will work with an SQL backbone, but NOT hdf5, as 2 tasks running on the same hdf5 file will collide!
    subprocess.run([executable,'-e',str(event_id),'-j',str(job_id)],cwd=dataroot)
    return

def fire(event):
    event_name = event['name'].values[0]
    event_value = event['value'].values[0]
    event_id = event['event_id'].values[0]
    #print("HERE ",event['name'].values[0]," DONE")
    parent_job = Job.get(int(event.job_id))
    conf_id = int(parent_job.config_id)
    configuration = Configuration.get(conf_id)
    pipeline_id = parent_job.pipeline_id
    #print(pipeline_id)
    alltasks =  Store().select('tasks',where="pipeline_id=="+str(pipeline_id))
    for i in range(alltasks.shape[0]):
        task = alltasks.iloc[i]
        task_id = task['task_id']
        #print(task_id)
        m = Store().select('masks',where="task_id=="+str(task_id))
        for j in range(m.shape[0]):
            mask = m.iloc[j]
            mask_name = mask['name']
            mask_value = mask['value']
    
            #print("HERE",event_name,mask_name,event_value,mask_value,"DONE3")
            if (event_name == mask_name) & ((event_value == mask_value) | (mask_value=='*')):
                taskname = task['name']
                newjob = Job(task=task,config=configuration,event_id=event_id).create() #need to give this a configuration
                job_id = int(newjob['job_id'].values[0])
                event_id = int(event['event_id'].values[0])
                print(taskname,"-e",event_id,"-j",job_id)
                Submit(task,job_id,event_id) #pipeline should be able to run stuff and keep track if it completes
                return

def logprint(configuration,job,log_text):
    target_id = configuration['target_id'].values[0]
    pipeline_id = configuration['pipeline_id'].values[0]
    print("T",target_id,"P",pipeline_id)
    myPipe = Pipeline.get(pipeline_id)
    myTarg = Target.get(target_id)
    conf_name = configuration['name'].values[0]
    targ_name = myTarg['name']
    logpath = myPipe.data_root+'/'+targ_name+'/log_'+conf_name+'/'
    job_id = job['job_id']
    event_id = job['event_id']
    task_id = job['task_id']
    task = Task.get(task_id)
    task_name = task['name']
    logfile = task_name+'_j'+str(job_id)+'_e'+str(event_id)+'.log'
    print(task_name,job_id,event_id,logpath)
    try:
     log = open(logpath+logfile, "a")
    except:
     log = open(logpath+logfile, "w")
    log.write(log_text)
    log.close()
