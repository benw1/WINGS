#! /usr/bin/env python
import argparse,os,subprocess
from wpipe import *

def register(PID,task_name):
   myPipe = Store().select('pipelines').loc[int(PID)]
   myTask = Task(task_name,myPipe).create()
   _t = Task.add_mask(myTask,'*','start',task_name) 
   return

def discover_targets(pipeline):
   #start_targ = pipeline.getStartTarg() #get generic target in pipeline (unsorted?)
   #start_conf = start_targ.getStartConf() #get that target's configuration
   #datapath = pipeline.getrawpath() #get the path to that target's raw data directory
   #data = glob(datapath . '*.cat') #get a list of all the catalog files in that directory
   #Figure out RA and DEC and other attributes of data
   #number = -1;
   #for dat in data:
   #   Determine a target name
   #   if target_name not in target_names:
   #      target_names = append(target_names,target_name)
   #      number++
   #      sorted_data[number,0] = dat # add data to the stucture to be returned
   #   else:
   #      determine the index of target_name in target_names
   #      sorted_data[target_index,next] = dat #add data to the correct place in structure 
   #   start_targ.addDataProduct(give it the information)
   #divide into targets
   #return target_names, sorted_data, start_conf #structure of sorted_data will be multi-dimensional
   #                                             #so that each target can have multiple data files
   pass


def create_target(target_name,data,configuation):
   #new_target = pipeline.createTarget(target_name,configuration)
   #thisjob.createOption(targetname . "_data_processed", 0) #create an option for this target in this job that keeps track of the processed catalogs within the target.  It is important that each target get its own job option.  process_catalogs will increment this by one.
   #for dat in data:
   #     new_dat = dat.CopyTo(new_target,"raw")
   #     number = len(data) 
   #     event = create_event("new_data") #create "new_data" event
   #     event.addOption("data_id", new_dat.getID())
   #     event.addOption("processed", targetname . "_data_processed")
   #     event.addOption("total_data", number)
   #     event.fire()
   pass
   
    
def parse_all():
    parser = argparse.ArgumentParser()
    parser.add_argument('--R','-R', dest='REG', action='store_true',
                        help='Specify to Register')
    parser.add_argument('--P','-p',type=int,  dest='PID',
                        help='Pipeline ID')
    parser.add_argument('--E','-e',type=int,  dest='event_id',
                        help='Event ID')
    parser.add_argument('--J','-j',type=int,  dest='job_id',
                        help='Job ID')
    parser.add_argument('--N','-n',type=str,  dest='task_name',
                        help='Name of Task to be Registered')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_all()
    if args.REG:
       _t = register(args.PID,args.task_name)
    else:
       event = Event.getEvent(event_id) # get event object
       pipeline_id = event.getvalue("pipeline_id") #all events should know what pipeline they belong to
       pipeline = Source().pipeline(pipeline_id)
       targs,data,config = discover_targets(pipeline) #use catalogs in the pipeline's raw data dir to define targets
       count = 0;
       for targ in targs:
          pass
          #create_target(targ,data[count,:],config)  #given the target name, data, and configuration, create the target in the DB and kick off it's processing
          #count++
