#! /usr/bin/env python
from wpipe import *

def register(PID,task_name):
   
   myPipe = Store().select('pipelines','pipeline_id==$PID')
   myTask = Task(task_name,myPipe).create()
   myMask = Task.add_mask(myTask,'*','start','*').create()
   myMask = Task.add_mask(myTask,'test_wpipe.py','test','*').create()

'''

def discover_targets:
   pipeline = get pipeline()
   start_targ = pipeline.getStartTarg()
   start_conf = start_targ.getStartConf()
   datapath = pipeline.getrawpath()
   data = glob(datapath . '*.cat')
   Figure out RA and DEC and other attributes of data
   number = -1;
   for dat in data:
      Determine a target name
      if target_name not in target_names:
         target_names = append(target_names,target_name)
         number++
         sorted_data[number,0] = dat # add data to the stucture to be returned
      else:
         determine the index of target_name in target_names
	 sorted_data[target_index,next] = dat #add data to the correct place in structure 
      start_targ.addDataProduct(give it the information)
   divide into targets
   return target_names, sorted_data, start_conf #structure of sorted_data will be multi-dimensional

def create_target(target_name,data,configuation):
   new_target = pipeline.createTarget(target_name,configuration)
   thisjob.createOption(targetname . "_data_processed", 0)
   for dat in data:
        new_dat = dat.CopyTo(new_target,"raw")
	number = len(data) 
	event = create_event("new_data")
        event.addOption("data_name", new_dat.getName())
        event.addOption("total data", number)
        event.fire()

'''		

    
def parse_all():
    parser = argparse.ArgumentParser()
    parser.add_argument('-R',type=bool,dest='REG',default=False,help='To Register'
    parser.add_argument('-P',type=int,dest='PID',help='Pipeline ID'
    parser.add_argument('-N',type=str,dest='task_name',help='Name of Task to be Registered'


if __name__ == '__main__':
    args = parse_all()
    if args.REG:
       register(PID,task_name)
       return None

    config = pipeline
    targs,data,config = discover_targets()
    count = 0;
    for targ in targs:
       create_target(targ,data[count,:],config)
       count++
