#! /usr/bin/env python
import argparse,os,subprocess
from wpipe import *
from wingtips import WingTips as wtips
from wingtips import time, np, ascii


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
   myConfig = Configuration.get(int(catalogDP.config_id))
   myParams = Parameters.getParam(int(myConfig.config_id))
   
   fileroot = str(catalogDP.relativepath)
   filename = str(catalogDP.filename)     # For example:  'h15.shell.5Mpc.in'

   _t = subprocess.run(['cp',fileroot+'/'+filename,myConfig.procpath+'/.',stdout=subprocess.PIPE])
   # 
   fileroot = myConfig.procpath+'/'
   
   #filternames = myParams[filternames]
   filternames   = ['X625','Z087','Y106','J129','H158','F184']
   stips_files = read_match(fileroot+filename,filternames)
   comp_name = 'completed'+targ
   options = {comp_name:0}
   _opt = Options(options).create('job',job_id)

   for stips_cat in stips_files:
       _dp = DataProduct(filename=stips_cat,relativepath=myConfig.procpath,group='proc',configuration=myConfig).create()
       dpid = int(_dp.dp_id)
       event = Job.getEvent(job,'new_stips_catalog',options={'dp_id':dpid,'to_run':total,'name':comp_name})
       fire(event)

def read_match(file,cols):
   data = np.loadtxt(file)
   nstars = len(data[:,0])
   tot_dens = np.float(nstars)/area
   print("MAX TOTAL DENSITY = ",tot_dens)
   count = -1
   for col in (cols):
      count += 1
      if (col == 'H158'):
         print("H is column ",count)
         hcol = count
      if (col == 'X625'):
         print("X is column ",count)
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
   max_den = np.float(htot)/area
   del h
   print("MAX H(23-24) DENSITY = ",max_den)

   stips_in = []
 
   M1, M2, M3, M4, M5 =  data[:,zcol], data[:,ycol], data[:,jcol], data[:,hcol],mydata[:,fcol]
   radist = np.abs(1/((mytot_dens**0.5)*np.cos(deccent*3.14159/180.0)))/3600.0
   decdist = (1/mytot_dens**0.5)/3600.0
   print('RA',radist,'DEC',decdist)
   coordlist = np.arange(np.rint(np.float(len(M2))**0.5)+1)
   np.random.shuffle(coordlist)
   print(radist,decdist)
   ra = 0.0
   dec = 0.0
   for k in range(len(coordlist)):
      ra = np.append(ra,radist*coordlist+racent-(pix*1024.0/3600.0))
      dec = np.append(dec,np.repeat(decdist*coordlist[k]+deccent-(pix*1024.0/3600.0),len(coordlist)))
   ra = ra[1:len(M1)+1]
   dec = dec[1:len(M1)+1]
   print(len(ra),len(M1))
   M = np.array([M1,M2,M3,M4,M5]).T
   del M1,M2,M3,M4,M5
   file1 = file.split('.')
   file2 = '.'.join(file1[0:len(file1)-1])
   file3 = file2+str(np.around(hden,decimals=5))+'.'+file1[-1]
   stips_lists = write_stips(file3,ra,dec,M)
   del M
   gc.collect()
   stips_in.append(stips_lists)
   return stips_in

def write_stips(infile,ra,dec,M):
   filternames   = ['Z087','Y106','J129','H158','F184']
   ZP_AB = np.array([26.365,26.357,26.320,26.367,25.913])
   fileroot=infile
   starpre = '_'.join(infile.split('.')[:-1])
   filedir = infile.split('/')[0]+'/'
   outfiles = []
   for j,filt in enumerate(filternames):
        
      outfile = starpre+'_'+filt[0]+'.tbl'
      outfilename = outfile.split('/')[-1]
      flux    = wtips.get_counts(M[:,j],ZP_AB[j])
      print(M[-1,j])
      print(flux,ra,dec)
      # This makes a stars only input list
      wtips.from_scratch(flux=flux,ra=ra,dec=dec,outfile=outfile)
      stars = wtips([outfile])
      galaxies = wtips([filedir+filt+'.txt']) # this file will be provided pre-made
      galaxies.flux_to_Sb()                             # galaxy flux to surface brightness
      radec = galaxies.random_radec_for(stars)          # random RA DEC across star field
      galaxies.replace_radec(radec)                     # distribute galaxies across starfield
      stars.merge_with(galaxies)                        # merge stars and galaxies list
      outfile = filedir+'Mixed'+'_'+outfilename
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
      outfiles = outfiles.append(outfile)
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
      process_catalog(job_id,event_id,dp_id)
       
   # placeholder for additional steps
   print('done')
