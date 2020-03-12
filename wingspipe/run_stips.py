#! /usr/bin/env python
import argparse
import os
import subprocess

import wpipe as wp

# from stips.observation_module import ObservationModule #save for when ready

on_hyak = False
on_pbs = False
filtdict = {'R':'F062',
            'Z':'F087',
            'Y':'F106',
            'J':'F129',
            'H':'F158',
            'F':'F184'}


def register(task):
    _temp = task.mask(source='*', name='start', value=task.name)
    _temp = task.mask(source='*', name='new_stips_catalog', value='*')


def hyak_stips(job_id,event_id,dp_id,stips_script):
   myJob  = wp.Job.get(job_id)
   myPipe = wp.Pipeline.get(int(myJob.pipeline_id))

   catalogID = wp.Options.get('event',event_id)['dp_id']
   catalogDP = wp.DataProduct.get(int(catalogID))
   myTarget = wp.Target.get(int(catalogDP.target_id))
   myConfig = wp.Configuration.get(int(catalogDP.config_id))
   myParams = wp.Parameters.getParam(int(myConfig.config_id))

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
              'source activate forSTIPS3'+'\n'+
              'python '+stips_script)
   subprocess.run(['sbatch',slurmfile],cwd=myConfig.procpath)

def pbs_stips(job_id,event_id,dp_id,stips_script):
   myJob  = wp.Job.get(job_id)
   myPipe = wp.Pipeline.get(int(myJob.pipeline_id))
   dataroot = myPipe.data_root

   catalogID = wp.Options.get('event',event_id)['dp_id']
   catalogDP = wp.DataProduct.get(int(catalogID))
   myTarget = wp.Target.get(int(catalogDP.target_id))
   myConfig = wp.Configuration.get(int(catalogDP.config_id))
   myParams = wp.Parameters.getParam(int(myConfig.config_id))
   fileroot = str(catalogDP.relativepath)
   filename = str(catalogDP.filename) # for example, Mixed_h15_shell_3Mpc_Z.tbl
   filebase = filename.split('.')[0]
   filtroot = filename.split('_')[-1].split('.')[0]
   filtername = filtdict[filtroot]
   #pbsfile = stips_script+'.pbs'
   pbsfile = '/home1/bwilli24/Wpipelines/run_stips_jobs'
   #with open(pbsfile, 'w') as f:
   with open(pbsfile, 'a') as f:
      f.write(#'#PBS -S /bin/bash' + '\n'+
              #'#PBS -j oe' + '\n'+
              #'#PBS -l select=1:ncpus=4:model=san' + '\n'+
              #'#PBS -W group_list=s1692' + '\n'+
              #'#PBS -l walltime=10:00:00' + '\n'+
              #'source ~/.bashrc' + '\n' +
              #'cd ' + myConfig.procpath  + '\n'+

              #'source /nobackupp11/bwilli24/miniconda3/bin/activate STIPS'+'\n'+
              #'source /nobackupp11/bwilli24/miniconda3/bin/activate STIPS && '+'cd /tmp' +
              #' && python '+stips_script+' && mv /tmp/sim*'+str(dp_id)+'*fits '+myConfig.procpath+' && mv /tmp/'+filebase+'* '+myConfig.procpath+'\n')
              'source /nobackupp11/bwilli24/miniconda3/bin/activate STIPS && '+'cd '+myConfig.procpath+
              ' && python '+stips_script+'\n')
              #'python '+stips_script)
   return
   #subprocess.run(['qsub',pbsfile],cwd=myConfig.procpath)


def run_stips(job_id,event_id,dp_id,ra_dither,dec_dither,run_id):
   myJob  = wp.Job.get(job_id)
   myPipe = wp.Pipeline.get(int(myJob.pipeline_id))

   catalogID = dp_id
   catalogDP = wp.DataProduct.get(int(catalogID))
   myTarget = wp.Target.get(int(catalogDP.target_id))
   myConfig = wp.Configuration.get(int(catalogDP.config_id))
   myParams = wp.Parameters.getParam(int(myConfig.config_id))
   racent = float(myParams['racent'])+(float(ra_dither)/3600.0)
   deccent = float(myParams['deccent'])+(float(dec_dither)/3600.0)
   try:
      pa = myParams['pa']
   except:
      pa = 0.0
   fileroot = str(catalogDP.relativepath)
   filename = str(catalogDP.filename) # for example, Mixed_h15_shell_3Mpc_Z.tbl 
   
   filtroot = filename.split('_')[-1].split('.')[0]
   filtername = filtdict[filtroot]
   stips_script = myConfig.confpath+'/run_stips_'+str(dp_id)+'.py'
   with open(stips_script, 'w') as f:
      f.write('from stips.observation_module import ObservationModule'+'\n'+'import numpy as np\nfilename = \''+fileroot+'/'+filename+'\'\n'+'seed = np.random.randint(9999)+1000'+'\n'
'with open(filename) as myfile:'+'\n'+'   head = [next(myfile) for x in range(3)]'+'\n'+'pos = head[2].split(\' \')'+'\n'+'crud,ra = pos[2].split(\'(\')'+'\n'+'dec,crud =  pos[4].split(\')\')'+'\n'+'print(\"Running \",filename,ra,dec)'+'\n'+'print(\"SEED \",seed)'+'\n'+'scene_general = {\'ra\': '+str(racent)+',\'dec\': '+str(deccent)+',\'pa\': '+str(pa)+',\'seed\': seed}'+'\n'+'obs = {\'instrument\': \'WFI\', \'filters\': [\''+filtername+'\'], \'detectors\': 1,\'distortion\': False, \'oversample\': '+myParams['oversample']+',\'pupil_mask\': \'\', \'background\': \'avg\',\'observations_id\': '+str(dp_id)+', \'exptime\': '+myParams['exptime']+',\'offsets\': [{\'offset_id\': '+str(run_id)+', \'offset_centre\': False,\'offset_ra\': 0.0, \'offset_dec\': 0.0, \'offset_pa\': 0.0}]}'+'\n'+'obm = ObservationModule(obs, scene_general=scene_general)'+'\n'+'obm.nextObservation()'+'\n'+'source_count_catalogues = obm.addCatalogue(str(filename))'+'\n'+'psf_file = obm.addError()'+'\n'+'fits_file, mosaic_file, params = obm.finalize(mosaic=False)'+'\n')
   if on_hyak:
      hyak_stips(job_id, event_id, dp_id, stips_script)
   elif on_pbs:
      pbs_stips(job_id,event_id,dp_id,stips_script)
   else:
      os.system("python " + stips_script)
   #dp_opt = Parameters.getParam(myConfig.config_id) # Attach config params used tp run sim to the DP
   
   _dp = wp.DataProduct(filename='sim_'+str(dp_id)+'_0.fits',relativepath=fileroot,group='proc',subtype='stips_image',filtername=filtername,ra=myParams['racent'], dec=myParams['deccent'],configuration=myConfig).create()

def parse_all():
    parser = wp.PARSER
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
      register(wp.SQLPipeline(int(args.PID)).task(name=str(args.task_name)))
   else:
      job_id = int(args.job_id)
      event_id = int(args.event_id)
      event = wp.Event.get(event_id)
      dp_id = wp.Options.get('event',event_id)['dp_id']
      parent_job_id = int(event.job_id)
      compname = wp.Options.get('event',event_id)['name']
      ra_dither = wp.Options.get('event',event_id)['ra_dither']
      dec_dither = wp.Options.get('event',event_id)['dec_dither']
      update_option = int(wp.Options.get('job',parent_job_id)[compname])
      run_stips(job_id,event_id,dp_id,float(ra_dither),float(dec_dither),update_option)
      update_option = update_option+1
      _update = wp.Options.addOption('job',parent_job_id,compname,update_option)
      to_run = int(wp.Options.get('event',event_id)['to_run'])
      completed = update_option
      thisjob = wp.Job.get(job_id)
      catalogID = wp.Options.get('event',event_id)['dp_id']
      catalogDP = wp.DataProduct.get(int(catalogID))
      thisconf = wp.Configuration.get(int(catalogDP.config_id))
      myTarget = wp.Target.get(int(thisconf.target_id))
      print(''.join(["Completed ",str(completed)," of ",str(to_run)]))
      wp.logprint(thisconf,thisjob,''.join(["Completed ",str(completed)," of ",str(to_run),"\n"]))
      if (completed>=to_run):
         wp.logprint(thisconf,thisjob,''.join(["Completed ",str(completed)," and to run is ",str(to_run)," firing event\n"]))
         DP = wp.DataProduct.get(int(dp_id))
         tid = int(DP.target_id)
         #image_dps = wp.DataProduct.get({relativepath==config.procpath,subtype=='stips_image'})
         path = thisconf.procpath
         image_dps=wp.Store().select('data_products', where='config_id=='+str(thisconf.config_id)+' & subtype=='+'"stips_image"')
         comp_name = 'completed'+myTarget['name']
         options = {comp_name:0}
         _opt = wp.Options(options).create('job',job_id)
         total = len(image_dps)
         #print(image_dps(0))
         for index, dps in image_dps.iterrows():
            print(dps)
            dpid = int(dps.dp_id)
            newevent = wp.Job.getEvent(thisjob,'stips_done',options={'target_id':tid,'dp_id':dpid,'name':comp_name,'to_run':total})
            #wp.fire(newevent)
            wp.logprint(thisconf,thisjob,'stips_done but not firing any events for now\n')
            #wp.logprint(thisconf,thisjob,''.join(["Event= ",str(event.event_id)]))

   
