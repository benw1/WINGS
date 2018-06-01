#! /usr/bin/env python

import time
import pandas as pd
import numpy as np
from wpipe2 import *

create_data_HDFstore('./data/temp1.h5')



myUser = User('rubab',3).create()

print(myUser)

myNode = Node('local',10).create()

print(myNode)

myOpt1 = {'a':1,'b':2,'c':'dah','d':500.3432}
myPipe, pipeOpt = Pipeline(user=myUser,name='test_pipe',pipeline_id=13,software_root='/usr/local/bin',
                 data_root='/usr/local/data',pipe_root='/usr/local/pipe',config_root='/usr/local/others',
                 description='Testing pipeline',state_id=4).create(myOpt1)

print(myPipe)

print(pipeOpt)


myOpt2 = {'a':1,'x':7,'c':'dah','d':400.3432}
myTarget, targOpt = Target(name='test_target',target_id=74,relativepath='/targets',pipeline=myPipe).create(myOpt2)

print(myTarget)

myConfig, configOpt = Configuration(name='test_config',relativepath='/configs',description='Testing config',target=myTarget,
                 config_id=102).create()

print(myConfig)

print(configOpt)

configOpt = Options.add_or_update(Options.add_or_update(pipeOpt,targOpt),configOpt)

print(configOpt)

myDP = DataProduct(filename='test_file.fits',relativepath='/Unknown',group='raw',
                    configuration=myConfig,dp_id=41,filtername='H158').create(ret_opt=False)

print(myDP)

myParams=Parameters(params={'x':20,'y':40,'z':'yada','C':11.243,'D':39.32,'test':False}).create(myConfig,ret_opt=False)

print(myParams)


myConfig2 = Configuration(name='test_config2',relativepath='/configs',description='Testing config',target=myTarget,
                 config_id=105).create(ret_opt=False)
myConfig3 = Configuration(name='test_config3',relativepath='/configs',description='Testing config',target=myTarget,
                 config_id=112).create(ret_opt=False)
myParams2=Parameters(params={'x':120,'y':50,'z':'yada','C':11.243,'D':49.32,'test':False}).create(myConfig2,ret_opt=False)
myParams3=Parameters(params={'w':220,'y':50,'z':'yada','C':43.243,'X':49.32,'test':False}).create(myConfig3,ret_opt=False)

myDP1 = DataProduct(filename='test_file1.fits',relativepath='/Unknown',group='raw',
                    configuration=myConfig,dp_id=1,filtername='H158').create(ret_opt=False)
myDP2 = DataProduct(filename='test_file23.txt',relativepath='/Unknown',group='conf',
                    configuration=myConfig,dp_id=2,filtername='').create(ret_opt=False)
myDP3 = DataProduct(filename='test_file34.cl',relativepath='/Unknown',group='proc',
                    configuration=myConfig2,dp_id=3,filtername='').create(ret_opt=False)
myDP4 = DataProduct(filename='test_file41.py',relativepath='/Unknown',group='proc',
                    configuration=myConfig2,dp_id=4,filtername='').create(ret_opt=False)
myDP5 = DataProduct(filename='test_file54.info',relativepath='',group='raw',
                    configuration=myConfig2,dp_id=5,filtername='').create(ret_opt=False)
myDP6 = DataProduct(filename='test_file62.log',relativepath='/Unknown',group='log',
                    configuration=myConfig3,dp_id=6,filtername='').create(ret_opt=False)
myDP7 = DataProduct(filename='test_file79.fits',relativepath='/Unknown',group='raw',
                    configuration=myConfig3,dp_id=7,filtername='Z087').create(ret_opt=False)
myDP8 = DataProduct(filename='test_file85.jpg',relativepath='/Unknown',group='proc',
                    configuration=myConfig3,dp_id=8,filtername='').create(ret_opt=False)

allConfig = pd.concat([myConfig,myConfig2,myConfig3]).sort_index(level=['pipeline_id','target_id','config_id'])

print(allConfig)

allParams = pd.concat([myParams,myParams2,myParams3]).sort_index(level=['pipeline_id','target_id','config_id'])

print(allParams)

allOpt = pd.concat([pipeOpt,targOpt,configOpt]).sort_index(level=['owner','owner_id'])

print(allOpt)

allDP = pd.concat([myDP,myDP1,myDP2,myDP3,myDP4,myDP5,myDP6,myDP7,myDP8]
                 ).sort_index(level=['pipeline_id','target_id','config_id','dp_id'])

print(allDP)

print(Parameters.get_params(allParams,myConfig2))

print(Options.get_opt(allOpt,'target',74))

myTask = Task(name='tag_image',task_id=11,pipeline=myPipe).create(ret_opt=False)

print(myTask)

myMask = Task.add_mask(task=myTask,source='oneTask',name='aName',value=12)

print(myMask)

myJob = Job(state='starting',event_id=5,task=myTask,config=myConfig3,node=myNode).create(ret_opt=False)

print(myJob)



to_data_HDFstore(allDP,'./data/temp2.h5','data_products')

testDP = from_data_HDFstore('./data/temp2.h5','data_products')

print(testDP)

