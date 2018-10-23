#! /usr/bin/env python
import argparse,os,subprocess
from wpipe import *
#from stips.observation_module import ObservationModule #save for when ready

filtdict = {'R':'R062',
            'Z':'Z087',
            'Y':'Y106',
            'J':'J129',
            'H':'H158',
            'F':'F184'}

def register(PID,task_name):
   myPipe = Pipeline.get(PID)
   myTask = Task(task_name,myPipe).create()
   _t = Task.add_mask(myTask,'*','start',task_name)
   _t = Task.add_mask(myTask,'*','new_stips_catalog','*')
   return

def hyak_stips(job_id,event_id,dp_id,stips_script):
   myJob  = Job.get(job_id)
   myPipe = Pipeline.get(int(myJob.pipeline_id))

   catalogID = Options.get('event',event_id)['dp_id']
   catalogDP = DataProduct.get(int(catalogID))
   myTarget = Target.get(int(catalogDP.target_id))
   myConfig = Configuration.get(int(catalogDP.config_id))
   myParams = Parameters.getParam(int(myConfig.config_id))

   fileroot = str(catalogDP.relativepath)
   filename = str(catalogDP.filename) # for example, Mixed_h15_shell_3Mpc_Z.tbl 
   
   filtroot = filename.split('_')[-1].split('.')[0]
   filtername = filtdict[filtroot]
   slurmfile = stips_script+'.slurm'
   with open(slurmfile, 'w') as f:
      f.write('#!/bin/bash' + '\n'+
              '## Job Name' + '\n'+
              '#SBATCH --job-name=stips'+str(dp_id) + '\n'+
              '## Allocation Definition ' + '\n'+
              '#SBATCH --account=astro' + '\n'+
              '#SBATCH --partition=astro' + '\n'+
              '## Resources' + '\n'+
              '## Nodes' + '\n'+
              '#SBATCH --ntasks=1' + '\n'+
              '## Walltime (3 hours)' + '\n'+
              '#SBATCH --time=10:00:00' + '\n'+
              '## Memory per node' + '\n'+
              '#SBATCH --mem=10G' + '\n'+
              '## Specify the working directory for this job' + '\n'+
              '#SBATCH --workdir='+myConfig.procpath + '\n'+
              '##turn on e-mail notification' + '\n'+
              '#SBATCH --mail-type=ALL' + '\n'+
              '#SBATCH --mail-user=benw1@uw.edu' + '\n'+
              'source activate forSTIPS'+'\n'+
              'python2.7 '+stips_script)
   subprocess.run(['sbatch',slurmfile],cwd=myConfig.procpath)

def run_stips(job_id,event_id,dp_id,run_id):
   myJob  = Job.get(job_id)
   myPipe = Pipeline.get(int(myJob.pipeline_id))

   catalogID = dp_id
   catalogDP = DataProduct.get(int(catalogID))
   myTarget = Target.get(int(catalogDP.target_id))
   myConfig = Configuration.get(int(catalogDP.config_id))
   myParams = Parameters.getParam(int(myConfig.config_id))

   fileroot = str(catalogDP.relativepath)
   filename = str(catalogDP.filename) # for example, Mixed_h15_shell_3Mpc_Z.tbl 
   
   filtroot = filename.split('_')[-1].split('.')[0]
   filtername = filtdict[filtroot]
   stips_script = myConfig.confpath+'/run_stips_'+str(dp_id)+'.py'
   with open(stips_script, 'w') as f:
      f.write('from stips.observation_module import ObservationModule'+'\n'+'import numpy as np\nfilename = \''+fileroot+'/'+filename+'\'\n'+'seed = np.random.randint(9999)+1000'+'\n'
'with open(filename) as myfile:'+'\n'+'   head = [next(myfile) for x in xrange(3)]'+'\n'+'pos = head[2].split(\' \')'+'\n'+'crud,ra = pos[2].split(\'(\')'+'\n'+'dec,crud =  pos[4].split(\')\')'+'\n'+'print \"Running \",filename,ra,dec'+'\n'+'print(\"SEED \",seed)'+'\n'+'scene_general = {\'ra\': '+myParams['racent']+',\'dec\': '+myParams['deccent']+',\'pa\': 0.0, \'seed\': seed}'+'\n'+'obs = {\'instrument\': \'WFI\', \'filters\': [\''+filtername+'\'], \'detectors\': 1,\'distortion\': False, \'oversample\': '+myParams['oversample']+',\'pupil_mask\': \'\', \'background\': \'avg\',\'observations_id\': '+str(dp_id)+', \'exp_time\': '+myParams['exptime']+',\'offsets\': [{\'offset_id\': '+str(run_id)+', \'offset_centre\': False,\'offset_ra\': 0.0, \'offset_dec\': 0.0, \'offset_pa\': 0.0}]}'+'\n'+'obm = ObservationModule(obs, scene_general=scene_general)'+'\n'+'obm.nextObservation()'+'\n'+'source_count_catalogues = obm.addCatalogue(str(filename))'+'\n'+'psf_file = obm.addError()'+'\n'+'fits_file, mosaic_file, params = obm.finalize(mosaic=False)'+'\n')
   hyak_stips(job_id,event_id,dp_id,stips_script)
   #dp_opt = Parameters.getParam(myConfig.config_id) # Attach config params used tp run sim to the DP
   
   #_dp = DataProduct(filename=fits_file,relativepath=fileroot,group='proc',subtype='stips_image',filtername=filtername,ra=myParams['racent'], dec=myParams['deccent'],configuration=myConfig).create()

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
      parent_job_id = int(event.job_id)
      compname = Options.get('event',event_id)['name']
      update_option = int(Options.get('job',parent_job_id)[compname])
      run_stips(job_id,event_id,dp_id,update_option)
      update_option = update_option+1
      _update = Options.addOption('job',parent_job_id,compname,update_option)
      to_run = int(Options.get('event',event_id)['to_run'])
      completed = update_option
      thisjob = Job.get(job_id)
      catalogID = Options.get('event',event_id)['dp_id']
      catalogDP = DataProduct.get(int(catalogID))
      thisconf = Configuration.get(int(catalogDP.config_id))
      myTarget = Target.get(int(thisconf.target_id))
      print(''.join(["Completed ",str(completed)," of ",str(to_run)]))
      logprint(thisconf,thisjob,''.join(["Completed ",str(completed)," of ",str(to_run),"\n"]))
      if (completed>=to_run):
         logprint(thisconf,thisjob,''.join(["Completed ",str(completed)," and to run is ",str(to_run)," firing event\n"]))
         DP = DataProduct.get(int(dp_id))
         tid = int(DP.target_id)
         #image_dps = DataProduct.get({relativepath==config.procpath,subtype=='stips_image'})
         path = thisconf.procpath
         image_dps=Store().select('data_products', where='target_id=='+str(tid)+' & subtype=='+'"stips_image"')
         comp_name = 'completed'+myTarget['name']
         options = {comp_name:0}
         _opt = Options(options).create('job',job_id)
         total = len(image_dps)
         #print(image_dps(0))
         for index, dps in image_dps.iterrows():
            print(dps)
            dpid = int(dps.dp_id)
            newevent = Job.getEvent(thisjob,'stips_done',options={'target_id':tid,'dp_id':dpid,'name':comp_name,'to_run':total})
            fire(newevent)           
            logprint(thisconf,thisjob,'stips_done\n')
            logprint(thisconf,thisjob,''.join(["Event= ",str(event.event_id)]))

   
