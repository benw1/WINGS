#! /usr/bin/env python
import argparse,os,subprocess
from wpipe import *
import glob

def register(PID,task_name):
   myPipe = Store().select('pipelines').loc[int(PID)]
   myTask = Task(task_name,myPipe).create()
   _t = Task.add_mask(myTask,'*','start',task_name)
   _t = Task.add_mask(myTask,'*','stips_done','*')
   return

def outimages(imagepath):
   front = imagepath.split('.fits')[0]
   chips = glob.glob(front+'.chip\*.fits')
   return chips

def prep_image(imagepath,filtername,config):
   
   _t1 = ['wfirstmask', imagepath]
   _t2 = ['splitgroups', imagepath]
   _t = subprocess.run(_t1, stdout=subprocess.PIPE)
   _t = subprocess.run(_t2, stdout=subprocess.PIPE)
   outimages = outimages(imagepath)
   for outimage in outimages:
      _t3 = ['calcsky XXX XXX',imagepath]
      _t = subprocess.run(_t3, stdout=subprocess.PIPE)
      filename = outimage.split('/')[-1]
      front = filename.split('.fits')[0]
      _dp = DataProduct(filename=filename,relativepath=config.procpath,group='proc',subtype='dolphot',filtername=filtername,configuration=config).create()
      skyname = front+'.sky.fits'
      _dp = DataProduct(filename=skyname,relativepath=config.procpath,group='proc',subtype='dolphot',filtername=filtername,configuration=config).create()


   
    
def parse_all():
    parser = argparse.ArgumentParser()
    parser.add_argument('--R','-R', dest='REG', action='store_true',
                        help='Specify to Register')
    parser.add_argument('--P','-p',type=int,  dest='PID',
                        help='Pipeline ID')
    parser.add_argument('--N','-n',type=str,  dest='task_name',
                        help='Name of Task to be Registered')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_all()
    if args.REG:
       _t = register(args.PID,args.task_name)
    else:
       job_id = int(args.job_id)
       thisjob = Job.get(job_id)
       event_id = int(args.event_id)
       event = Event.get(event_id)
       config = Configuration.get(int(event.config_id))
       tid = config.target_id
       target = Target.get(int(config.target_id))
       image_dps = DataProduct.get({relativepath==config.procpath,subtype=='stips_image'})
       for dps in image_dps: #may want to parallelize this in the future
          imagepath = config.procpath+'/'+dps.filename
          filtername = dps.filtername
          prep_image(imagepath,filtername,config)
          
       newevent = Job.getEvent(thisjob,'images_prepped',options={'target_id':tid})
       fire(newevent)
       logprint(config,thisjob,'images_prepped\n')
       logprint(config,thisjob,''.join(["Event= ",str(event.event_id)]))
