#! /usr/bin/env python
import argparse,os,subprocess
from wpipe import *
from wingtips import WingTips as wtips
from wingtips import time, np, ascii
import gc


def register(PID,task_name):
   myPipe = Pipeline.get(PID)
   myTask = Task(task_name,myPipe).create()
   _t = Task.add_mask(myTask,'*','start',task_name)
   _t = Task.add_mask(myTask,'*','new_match_catalog','*')
   return

def generate_coords(job_id,event_id,dp_id):
   myJob = Job.get(job_id)
   myPipe = Pipeline.get(int(myJob.pipeline_id))

   catalogDP = DataProduct.get(int(dp_id))
   myTarget = Target.get(int(catalogDP.target_id))
   #print("NAME",myTarget['name'])
   myConfig = Configuration.get(int(catalogDP.config_id))
   myParams = Parameters.getParam(int(myConfig.config_id))
   
   fileroot = str(catalogDP.relativepath)
   filename = str(catalogDP.filename)     # For example:  'h15.shell.5Mpc.in'
   filepath = fileroot+'/'+filename
   _t = subprocess.run(['cp',filepath,myConfig.procpath+'/.'],stdout=subprocess.PIPE)
   # 
   fileroot = myConfig.procpath+'/'
   procdp = DataProduct(filename=filename,relativepath=fileroot,group='proc',configuration=myConfig).create()
   #filternames = myParams[filternames]
   filternames   = ['R062','Z087','Y106','J129','H158','F184']
   ra, dec, galradec, star_coord_file, gal_coord_file = get_match_coo(procdp.relativepath[0]+'/'+procdp.filename[0],filternames,myConfig)
   radec = np.array([ra,dec]).T
   np.random.shuffle(radec)
   with open(star_coord_file, 'w') as f:
      for item in radec:
        f.write("%s\n" % item)
   with open(gal_coord_file, 'w') as f:
      for item in galradec:
        f.write("%s\n" % item)

   comp_name = 'completed'+myTarget['name']
   options = {comp_name:0}
   _opt = Options(options).create('job',job_id)
   total = len(filternames)
   _dp = DataProduct(filename=star_coord_file,relativepath=myConfig.procpath,group='proc',subtype='star_coord_file',configuration=myConfig).create()
   _dp = DataProduct(filename=gal_coord_file,relativepath=myConfig.procpath,group='proc',subtype='gal_coord_file',configuration=myConfig).create()
   for filt in filternames:
      event = Job.getEvent(myJob,'coords_ready',options={'dp_id':dp_id,'filter':filt,'to_run':total,'name':comp_name})
      fire(event)

def get_match_coo(filepath,cols,myConfig):
   data = np.loadtxt(filepath)
   nstars = len(data[:,0])
   M = data[:,0]
   myParams = Parameters.getParam(int(myConfig.config_id))
   area = float(myParams["area"])
   imagesize = float(myParams["imagesize"])
   background = myParams["background_dir"]
   tot_dens = np.float(nstars)/area
   print("TOTAL DENSITY = ",tot_dens)

   stips_in = []
 
   racent = float(myParams['racent'])
   deccent = float(myParams['deccent'])
   pix = float(myParams['pix'])
   radist = np.abs(1/((tot_dens**0.5)*np.cos(deccent*3.14159/180.0)))/3600.0
   decdist = (1/tot_dens**0.5)/3600.0
   print('RA',radist,'DEC',decdist)
   coordlist = np.arange(np.rint(np.float(nstars)**0.5)+1)
   #print(radist,decdist)
   ra = 0.0
   dec = 0.0
   for k in range(len(coordlist)):
      ra = np.append(ra,radist*coordlist+racent-(pix*imagesize/7200.0))
      dec = np.append(dec,np.repeat(decdist*coordlist[k]+deccent-(pix*imagesize/7200.0),len(coordlist)))
   ra = ra[1:nstars+1]
   dec = dec[1:nstars+1]
   filename = filepath.split('/')[-1]
   file1 = filename.split('.')
   file2 = '.'.join(file1[0:len(file1)-1])
   file3 = myConfig.procpath+'/'+file2+'.'+file1[-1]
   star_coord_file = myConfig.procpath+'/'+file2+'_stradec'+'.'+file1[-1]
   gal_coord_file = myConfig.procpath+'/'+file2+'_galradec'+'.'+file1[-1]
   galradec = getgalradec(filepath,ra,dec,M,background)
   return ra,dec,galradec,star_coord_file,gal_coord_file

def getgalradec(infile,ra,dec,M,background):
    filt = 'Z087'
    ZP_AB = np.array([26.365,26.357,26.320,26.367,25.913])
    fileroot=infile
    starpre = '_'.join(infile.split('.')[:-1])
    filedir = background+'/'
    outfile = starpre+'_'+filt+'.tbl'
    outfilename = outfile.split('/')[-1]
    flux    = wtips.get_counts(M,ZP_AB[0])
    wtips.from_scratch(flux=flux,ra=ra,dec=dec,outfile=outfile)
    stars = wtips([outfile])
    galaxies = wtips([filedir+filt+'.txt']) # this file will be provided pre-made
    radec = galaxies.random_radec_for(stars)
    return radec


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
   parser.add_argument('--DP','-dp',type=int,  dest='dp_id',
                       help='Dataproduct ID')
   return parser.parse_args()

if __name__ == '__main__':
   args = parse_all()
   if args.REG:
      _t = register(int(args.PID),str(args.task_name))
   else:
      job_id = int(args.job_id)
      event_id = int(args.event_id)
      event = Event.get(event_id)
      dp_id = Options.get('event',event_id)['dp_id']
      generate_coords(job_id,event_id,dp_id)
      
   
