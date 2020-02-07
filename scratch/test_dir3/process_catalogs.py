#! /usr/bin/env python
import argparse,os,subprocess
from wpipe import *
from wingtips import WingTips as wtips
from wingtips import time, np, ascii


def register(PID,task_name):
   myPipe = Pipeline.get(PID)
   myTask = Task(task_name,myPipe).create()
   _t = Task.add_mask(myTask,'*','start',task_name)
   _t = Task.add_mask(myTask,'*','new_catalog','*')
   return

def process_catalog(job_id,event_id):
   myJob = Job.get(job_id)
   myPipe = Pipeline.get(int(myJob.pipeline_id))

   catalogID = Options.get('event',event_id)['dp_id']
   catalogDP = DataProduct.get(int(cat_id))
   myTarget = Target.get(int(catalogDP.target_id))
   myConfig = Configuration.get(int(catalogDP.config_id))
   myParams = Parameters.getParam(int(myConfig.config_id))
   
   fileroot = str(catalogDP.relativepath)
   filename = str(catalogDP.filename)     # For example:  'h15.shell.5Mpc.in'

   _t = subprocess.call(['cp',fileroot+'/'+filename,
                         myTarget.relativepath+'/proc_'+myConfig['name']+'/.',
                         stdout=subprocess.PIPE])
   # 
   fileroot = myTarget.relativepath+'/proc_'+myConfig['name']+'/'
   
   #filternames = myParams[filternames]
   filternames   = ['Z087','Y106','J129','H158','F184']

   #ZP_AB = myParams[ZP_AB]
   ZP_AB = np.array([26.365,26.357,26.320,26.367,25.913])
   
   dist = float(infile.split('.')[2][:-3])    # We will have it in the config
   starpre = '_'.join(infile.split('.')[:-1])
   data = ascii.read(infile)
   RA, DEC, M1, M2, M3, M4, M5 = \
      data['col1'], data['col2'], data['col3'], data['col4'],\
      data['col5'], data['col6'], data['col7']
   M = np.array([M1,M2,M3,M4,M5]).T

   stips_inputs = []
   
   for j,filt in enumerate(filternames):
      outfile = starpre+'_'+filt[0]+'.tbl'
      flux    = wtips.get_counts(M[:,j],ZP_AB[j],dist=dist)
      # This makes a stars only input list
      wtips.from_scratch(flux=flux, 
                         ra=RA,dec=DEC,
                         outfile=fileroot+outfile)
      _dp = DataProduct(filename=outfile,
                        relativepath=fileroot,
                        group='proc',subtype='star_cat',
                        configuration=myConfig)\
                        .create()
      stars = wtips([fileroot+outfile])
      galaxies = wtips([fileroot+filt+'_galaxies.txt']) # this file will be provided pre-made
      galaxies.flux_to_Sb()                             # galaxy flux to surface brightness
      radec = galaxies.random_radec_for(stars)          # random RA DEC across star field
      galaxies.replace_radec(radec)                     # distribute galaxies across starfield
      stars.merge_with(galaxies)                        # merge stars and galaxies list
      outfile = 'Mixed'+'_'+outfile
      stars.write_stips(fileroot+outfile,ipac=True)
      if j==0:
         COnfiguration.addParam(myConfig,'RA', str(stars.center[0])
         COnfiguration.addParam(myConfig,'DEC', str(stars.center[1]))
      with open(fileroot+outfile, 'r+') as f:
         content = f.read()
         f.seek(0, 0)
         f.write('\\type = internal' + '\n'  +
                 '\\filter = ' + str(filt) +'\n' + 
                 '\\center = (' + str(stars.center[0]) +
                 '  ' + str(stars.center[1]) + ')\n' +
                 content)
      _dp = DataProduct(filename=outfile,
                        relativepath=fileroot,
                        group='proc',subtype='stips_input'
                        configuration=myConfig)\
                        .create()
         
      stips_inputs.append(_dp.dp_id)

      
   event = Job.getEvent(myJob,'new_stips_input',
                        options={'dp_id': stips_inputs,
                                 'to_run':int(len(stips_inputs)),
                                 'completed': 0})
   
   # This will start N run_stips jobs, one for each dp_id
   # Event.fire(event)

   Event.run_complete(Event.get(int(event_id)))

   _parent = Options.get('event',event_id)
   to_run,completed = int(_parent['to_run']), int(_parent['completed'])

   if !(completed<to_run):
      event = Job.getEvent(myJob,'process_catalogs_completed')
      # Event.fire(event)

   return None
    
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
      dp_id = int(args.dp_id)
      process_catalog(job_id,event_id,dp_id)
       
   # placeholder for additional steps
   print('done')
