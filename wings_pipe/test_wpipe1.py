#! /usr/bin/env python

from wpipe import *


myUser = User('rubab').create()

myUser = User('ben').create()

myUser = User('xoxo').create()

myNode = Node('local').create()

myOpt1 = {'a':1,'b':2,'c':'dah','d':500.3432}

myPipe = Pipeline(user=myUser,name='test_pipe',software_root='/usr/local/bin',
                 data_root='/usr/local/data',pipe_root='/usr/local/pipe',config_root='/usr/local/others',
                 description='Testing pipeline').create()


myTarget, targOpt = Target(name='test_target',relativepath='/targets',
                           pipeline=myPipe).create(options=myOpt1)

myConfig, configOpt = Configuration(name='test_config',relativepath='/configs',
                                    description='Testing config',
                                    target=myTarget).create(options=myOpt1)

myDP = DataProduct(filename='test_file.fits',relativepath='/Unknown',group='raw',
                    configuration=myConfig,filtername='H158').create(ret_opt=False)

myTask = Task(name='tag_image',pipeline=myPipe).create()

myMask = Task.add_mask(task=myTask,source='oneTask',name='aName',value=12)

myJob = Job(state='starting',task=myTask,config=myConfig,node=myNode).create(ret_opt=False)

myEvent = Event(job=myJob,name='bName',value=4,jargs='').create()

myDP1 = DataProduct(filename='test_file1.fits',relativepath='/Unknown',group='raw',
                    configuration=myConfig,filtername='H158').create(ret_opt=False)
myDP2 = DataProduct(filename='test_file23.txt',relativepath='/Unknown',group='conf',
                    configuration=myConfig,filtername='').create(ret_opt=False)
myDP3 = DataProduct(filename='test_file34.cl',relativepath='/Unknown',group='proc',
                    configuration=myConfig,filtername='').create(ret_opt=False)
myDP4 = DataProduct(filename='test_file41.py',relativepath='/Unknown',group='proc',
                    configuration=myConfig,filtername='').create(ret_opt=False)
myDP5 = DataProduct(filename='test_file54.info',relativepath='',group='raw',
                    configuration=myConfig,filtername='').create(ret_opt=False)
myDP6 = DataProduct(filename='test_file62.log',relativepath='/Unknown',group='log',
                    configuration=myConfig,filtername='').create(ret_opt=False)
myDP7 = DataProduct(filename='test_file79.fits',relativepath='/Unknown',group='raw',
                    configuration=myConfig,filtername='Z087').create(ret_opt=False)
myDP8 = DataProduct(filename='test_file85.jpg',relativepath='/Unknown',group='proc',
                    configuration=myConfig,filtername='').create(ret_opt=False)
