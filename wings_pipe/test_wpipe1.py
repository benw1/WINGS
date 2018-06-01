#! /usr/bin/env python
from wpipe import *

myUser = User('rubab',3)
myOpt = Options({'a':1,'b':2,'c':'dah','d':500.3432})
myPipe = Pipeline(user=myUser,name='test_pipe',pipeline_id=13,software_root='/usr/local/bin',
                 data_root='/usr/local/data',pipe_root='/usr/local/pipe',configuration_root='/usr/local/others',
                 description='Testing pipeline',state_id=4,options=myOpt)
myTarget = Target(name='test_target',target_id=74,relativepath='/targets',pipeline=myPipe)
myConfig = Configuration(name='test_config',relativepath='/configs',description='Testing config',target=myTarget,
                 config_id=102)
myDP = DataProduct(filename='test_file1.fits',relativepath='/Unknown',group='raw',
                    configuration=myConfig,dp_id=41,filtername='H158')
myNode = Node('local',10)
dp_copy_state = CopyState(dp=myDP,node=myNode,state='initial',state_id=4)

myParams = Parameters(config=myConfig,params={'x':20,'y':40,'z':'yada','C':11.243,'D':39.32,'test':False})

myTask = Task(name='tag_image',task_id=11,pipeline=myPipe)
myMask = Mask(source='someTask',name='someName',value=100)
myTask.addMask(myMask)

print(to_dict(myTask))

print(to_dict(myTask.options))

print(to_dict(myTask.pipeline))

print(to_dict(myTask.pipeline.options))

print(to_dict(myTask.pipeline.user))

######

myDP.__dict__['sciType']='clf'

print(myDP.sciType)


