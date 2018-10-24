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

def process_match_catalog(job_id,event_id,dp_id):
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
   stips_files = read_match(procdp.relativepath[0]+'/'+procdp.filename[0],filternames,myConfig)
   comp_name = 'completed'+myTarget['name']
   options = {comp_name:0}
   _opt = Options(options).create('job',job_id)
   total = len(stips_files)
   for stips_cat in stips_files:
      _dp = DataProduct(filename=stips_cat,relativepath=myConfig.procpath,group='proc',configuration=myConfig).create()
      dpid = int(_dp.dp_id)
      event = Job.getEvent(myJob,'new_stips_catalog',options={'dp_id':dpid,'to_run':total,'name':comp_name})
      fire(event)

def read_match(filepath,cols,myConfig):
   data = np.loadtxt(filepath)
   nstars = len(data[:,0])
   myParams = Parameters.getParam(int(myConfig.config_id))
   area = float(myParams["area"])
   background = myParams["background_dir"]
   tot_dens = np.float(nstars)/area
   print("MAX TOTAL DENSITY = ",tot_dens)
   count = -1
   for col in (cols):
      count += 1
      if (col == 'H158'):
         print("H is column ",count)
         hcol = count
      if (col == 'R062'):
         print("R is column ",count)
         xcol = count
      if (col == 'Y106'):
         print("Y is column ",count)
         ycol = count
      if (col == 'Z087'):
         print("Z is column ",count)
         zcol = count
      if (col == 'J129'):
         print("J is column ",count)
         jcol = count
      if (col == 'F184'):
         print("F is column ",count)
         fcol = count
   h = data[:,hcol]
   htot_keep = (h > 23.0) & (h < 24.0)
   hkeep = h[htot_keep]
   htot = len(hkeep)
   hden = np.float(htot)/area
   del h
   print("H(23-24) DENSITY = ",hden)

   stips_in = []
 
   M1, M2, M3, M4, M5 =  data[:,zcol], data[:,ycol], data[:,jcol], data[:,hcol],data[:,fcol]
   racent = float(myParams['racent'])
   deccent = float(myParams['deccent'])
   pix = float(myParams['pix'])
   radist = np.abs(1/((tot_dens**0.5)*np.cos(deccent*3.14159/180.0)))/3600.0
   decdist = (1/tot_dens**0.5)/3600.0
   print('RA',radist,'DEC',decdist)
   coordlist = np.arange(np.rint(np.float(len(M2))**0.5)+1)
   np.random.shuffle(coordlist)
   #print(radist,decdist)
   ra = 0.0
   dec = 0.0
   for k in range(len(coordlist)):
      ra = np.append(ra,radist*coordlist+racent-(pix*1024.0/3600.0))
      dec = np.append(dec,np.repeat(decdist*coordlist[k]+deccent-(pix*1024.0/3600.0),len(coordlist)))
   ra = ra[1:len(M1)+1]
   dec = dec[1:len(M1)+1]
   #print(len(ra),len(M1))
   M = np.array([M1,M2,M3,M4,M5]).T
   del M1,M2,M3,M4,M5
   filename = filepath.split('/')[-1]
   file1 = filename.split('.')
   file2 = '.'.join(file1[0:len(file1)-1])
   file3 = myConfig.procpath+'/'+file2+str(np.around(hden,decimals=5))+'.'+file1[-1]
   #print("STIPS",file3)
   galradec = getgalradec(file3,ra,dec,M,background)
   stips_lists = write_stips(file3,ra,dec,M,background,galradec)
   del M
   gc.collect()
   stips_in = np.append(stips_in,stips_lists)
   return stips_in

def getgalradec(infile,ra,dec,M,background):
    filt = 'Z087'
    ZP_AB = np.array([26.365,26.357,26.320,26.367,25.913])
    fileroot=infile
    starpre = '_'.join(infile.split('.')[:-1])
    filedir = background+'/'
    outfile = starpre+'_'+filt+'.tbl'
    outfilename = outfile.split('/')[-1]
    flux    = wtips.get_counts(M[:,0],ZP_AB[0])
    wtips.from_scratch(flux=flux,ra=ra,dec=dec,outfile=outfile)
    stars = wtips([outfile])
    galaxies = wtips([filedir+filt+'.txt']) # this file will be provided pre-made
    radec = galaxies.random_radec_for(stars)
    return radec


def write_stips(infile,ra,dec,M,background,galradec):
   filternames   = ['Z087','Y106','J129','H158','F184']
   ZP_AB = np.array([26.365,26.357,26.320,26.367,25.913])
   fileroot=infile
   starpre = '_'.join(infile.split('.')[:-1])
   filedir = '/'.join(infile.split('/')[:-1])+'/'
   outfiles = []
   for j,filt in enumerate(filternames):
        
      outfile = starpre+'_'+filt[0]+'.tbl'
      outfilename = outfile.split('/')[-1]
      flux    = wtips.get_counts(M[:,j],ZP_AB[j])
      # This makes a stars only input list
      wtips.from_scratch(flux=flux,ra=ra,dec=dec,outfile=outfile)
      stars = wtips([outfile])
      galaxies = wtips([background+'/'+filt+'.txt']) # this file will be provided pre-made
      galaxies.flux_to_Sb()                             # galaxy flux to surface brightness
      galaxies.replace_radec(galradec)                     # distribute galaxies across starfield
      stars.merge_with(galaxies)                        # merge stars and galaxies list
      outfile = filedir+'Mixed'+'_'+outfilename
      mixedfilename = 'Mixed'+'_'+outfilename
      stars.write_stips(outfile,ipac=True)
      with open(outfile, 'r+') as f:
         content = f.read()
         f.seek(0, 0)
         f.write('\\type = internal' + '\n'  +
                 '\\filter = ' + str(filt) +'\n' + 
                 '\\center = (' + str(stars.center[0]) +
               '  ' + str(stars.center[1]) + ')\n' +
                 content)
      f.close()
      del stars
      del galaxies
      gc.collect()
      outfiles = np.append(outfiles,mixedfilename)
   return outfiles
    
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
      process_match_catalog(job_id,event_id,dp_id)
      
   
