#! /usr/bin/env python
import argparse,os,subprocess
from astropy.io import fits
from wpipe import *
from shutil import which
import glob

def register(PID,task_name):
   myPipe = Store().select('pipelines').loc[int(PID)]
   myTask = Task(task_name,myPipe).create()
   _t = Task.add_mask(myTask,'*','start',task_name)
   _t = Task.add_mask(myTask,'*','stips_done','*')
   return

def outimages(imagepath):
   front = imagepath.split('.fits')[0]
   print("FRONT: ",front)
   chips = glob.glob(front+'.chip*.fits')
   print("CHIPS: ",chips,". \n")
   return chips

def prep_images(myConfig):
   myDP = Store().select('data_products').loc[int(myConfig.pipeline_id), int(myConfig.target_id),int(myConfig.config_id),:]
   imagedp = myDP[myDP['subtype']=='stips_image']
   _job = Job(config=myConfig).create() #need to create dummy job to keep track of events
   job_id = int(_job.job_id)
   myJob = Job.get(job_id)
   target = Target.get(int(myConfig.target_id))
   targ = target['name']
   print("TARGET NAME: ",targ,"\n")
   comp_name = 'completed'+targ
   options = {comp_name:0}
   _opt = Options(options).create('job',job_id)
   for dp_id in imagedp['dp_id']:
      print("DP_ID ",dp_id," sent\n")
      logprint(myConfig,myJob,''.join(["DP_ID ",str(dp_id)," sent\n"]))
      send(int(dp_id),myConfig,comp_name,len(imagedp['dp_id']),myJob) #send catalog to next step
   return
      
def send(dpid,conf,comp_name,total,job):
   dp = DataProduct.get(int(dpid))
   tid = conf.target_id
   filepath = dp.relativepath[0]+'/'+dp.filename[0]
   event = Job.getEvent(job,'stips_done',options={'dp_id':dpid,'target_id':tid,'to_run':total,'name':comp_name})
   logprint(conf,job,''.join(["Firing stips_done for ",str(filepath)," one of ",str(total),"\n"]))
   fire(event)
   return 


def prep_image(imagepath,filtername,config,thisjob,dp_id):
   logprint(config,thisjob,''.join(['running ',imagepath,' in filter ',filtername]))
   myParams=Parameters.getParam(int(config.config_id))
   dolphot_path = which('wfirstmask')
   dolphot_path = dolphot_path[:-10]
   targid = config.target_id
   target = Target.get(int(targid))
   targetname = target['name']
   print(targetname," TARGET\n")
   new_image_name = targetname+'_'+filtername+".fits"
   try:
      os.rename(imagepath,config.procpath+'/'+new_image_name)
   except: 
      pass
   imagepath = config.procpath+'/'+new_image_name
   #CHANGE FILENAME IN DATA PRODUCT.  ASK RUBAB
   dp = Store().select('data_products', 'dp_id=='+str(dp_id))
   dp['filename'] = new_image_name
   Store().update('data_products',dp)
   fixwcs(imagepath)
   _t1 = [dolphot_path+'wfirstmask','-exptime='+myParams['exptime'],"-rdnoise=41.73", imagepath]
   _t2 = [dolphot_path+'splitgroups', imagepath]
   _t = subprocess.run(_t1, stdout=subprocess.PIPE)
   _t = subprocess.run(_t2, stdout=subprocess.PIPE)
   outims = outimages(imagepath)
   if len(outims) > 1:
      print(outimages," Images\n")
      #for outimage in outimages:
      #placeholder for when there are 18 chips in each sim
   else:
      _t3 = [dolphot_path+'calcsky',imagepath,"15 35 -64 2.25 2.00"] #put in calcsky parameters
      _t = subprocess.run(_t3, stdout=subprocess.PIPE)
      filename = outims[0].split('/')[-1]
      front = filename.split('.fits')[0]
      _dp1 = DataProduct(filename=filename,relativepath=config.procpath,group='proc',subtype='dolphot_data',filtername=filtername,configuration=config).create()
      skyname = front+'.sky.fits'
      _dp2 = DataProduct(filename=skyname,relativepath=config.procpath,group='proc',subtype='dolphot_sky',filtername=filtername,configuration=config).create()

def fixwcs(imagepath):
   #use astropy to get CDELT1 and CDELT2 from header
   data, head = fits.getdata(imagepath, header=True)
   cd11 = head['PC1_1']
   cd22 = head['PC2_2']
   head['CD1_1']=cd11
   head['CD2_2']=cd22
   head['CD2_1']=0
   head['CD1_2']=0
   fits.writeto(imagepath,data,head,clobber=True)   
   return
               
def parse_all():
    parser = argparse.ArgumentParser()
    parser.add_argument('--R','-R', dest='REG', action='store_true',
                        help='Specify to Register')
    parser.add_argument('--P','-p',type=int,  dest='PID',
                        help='Pipeline ID')
    parser.add_argument('--N','-n',type=str,  dest='task_name',
                        help='Name of Task to be Registered')
    parser.add_argument('--E','-e',type=int,  dest='event_id',
                        help='Event ID')
    parser.add_argument('--J','-j',type=int,  dest='job_id',
                        help='Job ID')
    parser.add_argument('--C','-c',type=int,  dest='config_id',
                        help='Configuration ID')
    parser.add_argument('--T','-t',type=int,  dest='target_id',
                        help='Target ID')

    return parser.parse_args()

if __name__ == '__main__':
    args = parse_all()
    if args.REG:
       _t = register(args.PID,args.task_name)
    elif args.config_id:
       myConfig = Configuration.get(args.config_id)
       prep_images(myConfig)
    elif args.target_id:
       myTarget = Target.get(int(args.target_id))
       pid = myTarget.pipeline_id
       allConf = Store().select('configurations').loc[pid,int(args.target_id),:]
       for i in range(len(allConf)):
          myConfig = Configuration.get(int(allConf['config_id'][i]))
          print(myConfig)
          prep_images(myConfig)
    else:
       job_id = int(args.job_id)
       thisjob = Job.get(job_id)
       event_id = int(args.event_id)
       event = Event.get(event_id)
       print(thisjob.config_id)
       myConfig = Configuration.get(int(thisjob.config_id))
       tid = int(Options.get('event',event_id)['target_id'])
       dp_id = int(Options.get('event',event_id)['dp_id'])
       target = Target.get(int(myConfig.target_id))
       dp = DataProduct.get(dp_id)
       filtername = dp.filtername
       imagepath = dp.relativepath+'/'+dp.filename
       prep_image(imagepath,filtername,myConfig,thisjob,dp_id)
       parent_job_id = int(event.job_id)
       compname = Options.get('event',event_id)['name']
       update_option = int(Options.get('job',parent_job_id)[compname])
       update_option = update_option+1
       _update = Options.addOption('job',parent_job_id,compname,update_option)
       to_run = int(Options.get('event',event_id)['to_run'])
       completed = update_option
       thisjob = Job.get(job_id)
       catalogID = Options.get('event',event_id)['dp_id']
       catalogDP = DataProduct.get(int(catalogID))
       thisconf = Configuration.get(int(catalogDP.config_id))
       logprint(myConfig,thisjob,''.join(["Completed ",str(completed)," of ",str(to_run),"\n"]))
       if (completed>=to_run):
          newevent = Job.getEvent(thisjob,'images_prepped',options={'target_id':tid})
          fire(newevent)
          logprint(myConfig,thisjob,'images_prepped\n')
          logprint(myConfig,thisjob,''.join(["Event= ",str(newevent.event_id)," images_prepped\n"]))
