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

def run_stips(job_id,event_id,dp_id):
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

   scene_general = {'ra': myParams['racent'],
                    'dec': myParams['deccent'],
                    'pa': 0.0, 'seed': 1234}

   # We can pass all of these through configuration
   obs = {'instrument': 'WFI', 'filters': [filtername], 'detectors': 1,'distortion': False, 'oversample': 10,'pupil_mask': '', 'background': 'avg','observations_id': dp_id, 'exp_time': 10000,'offsets': [{'offset_id': 1, 'offset_centre': False,'offset_ra': 0.0, 'offset_dec': 0.0, 'offset_pa': 0.0}]}
   
   #obm = ObservationModule(obs, scene_general=scene_general)
   #obm.nextObservation()
   #source_count_catalogues = obm.addCatalogue(fileroot+filename)
   #psf_file = obm.addError()
   #fits_file, mosaic_file, params = obm.finalize(mosaic=False)

   #dp_opt = Configuration.getParams(myConfig) # Attach config params used tp run sim to the DP
   
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
      run_stips(job_id,event_id,dp_id)
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
      print(''.join(["Completed ",str(completed)," of ",str(to_run)]))
      logprint(thisconf,thisjob,''.join(["Completed ",str(completed)," of ",str(to_run)]))
      if (completed>=to_run):
         logprint(thisconf,thisjob,''.join(["Completed ",str(completed)," and to run is ",str(to_run)," firing event"]))
         DP = DataProduct.get(int(dp_id))
         tid = int(DP.target_id)
         newevent = Job.getEvent(thisjob,'stips_done',options={'target_id':tid})
         _job  = Job(event_id=int(newevent.event_id)).create()
         fire(newevent)
         logprint(thisconf,thisjob,'completed STIPS')
         logprint(thisconf,thisjob,''.join(["Event=",str(event.event_id),"; Job=",str(_job.job_id)]))

   
