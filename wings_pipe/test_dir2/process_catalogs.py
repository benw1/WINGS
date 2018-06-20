#! /usr/bin/env python
import argparse,os,subprocess
from wpipe import *

def register(PID,task_name):
   myPipe = Store().select('pipelines').loc[int(PID)]
   myTask = Task(task_name,myPipe).create()
   _t = Task.add_mask(myTask,'*','start',task_name)
   _t = Task.add_mask(myTask,'*','new_data',*)
   return

def process_catalog(data,configuration,previous_job,total,processed):
   filepath = data.getValue('path') #all data products should know their path
   new_dp = data.Copyto('proc') #copy data product to the proc/ directory and make new DP
   #Do standard things to the file to make STIPS input, which may produce more files
   #data = read(new_dp.path)
   #input_number = 0
   #for filts in range(number_of_filter_columns):  #generate STIPS input for each filter in catalog
   #    create new stips input file
   #    add in background, etc
   #    input_number++ #keep track of number of input stips files
   #    stips_in_dp = Source().createDataProduct(new file name, configuration, etc)	
   #    stips_in_dp.addValue("dataType","STIPSINPUT") #tag as a stips input file
   #keep track of total files in configuration
   #try:
   #    total_stips_input = configuration.getParameter("total_stips_input")
   #except:
   #    configuration.addParameter("total_stips_input", input_number) #if not defined, create the parameter
   #else:
   #	new_total = total_stips_input + input_number
   #    configuration.addParameter("total_stips_input", new_total) # if defined, update the parameter
   #
   #completed = previous_job.increment_option(processed) #increase the previous job's processed option and return the updated value
   #
   #if completed == total:  #check if all process_catalog jobs are done for this target
   #     stips_input_dps = Target.get_data_products(dataType == 'STIPSINPUT')
   #     if len(stips_input_dps) != new_total:
   #         Error!
   #     else:
   #         thisjob.createOption(targetname . "_stips_input", 0) #create an option for this target in this job that keeps track of the processed catalogs within the target.  It is important that each target get its own job option. run_stips will increment this by one.
   #         for i in range(len(stips_input_dps):
   #	         stips_in = stips_input_dps[i]
   #             stips_in_id = stips_in.getID()
   #	         new_event = Source().Event.create("new_stips_input") #create new_stips_input event
   #             new_event.addOption("data_id", stips_in_id)
   #             new_event.addOption("processed", targetname . "_stips_input")
   #             new_event.addOption("total_stips_in", number)
   #             new_event.fire()
        
   
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
       event = Source().Event(event_id)
       target_id = event.getValue('target_id')
       data_id = event.getValue('data_id')
       data = Source().DataProduct(data_id)
       parent_job_id = event.getValue('parent_job_id') #events should all know parent job id
       configuration_id = event.getValue('configuration_id') #events should all know their parent configuration
       configuration = Source().Configuration(configuration_id)
       parent_job = Source().Job(parent_job_id)
       total_data = event.getValue('total_data')
       processed = event.getVault('processed')
       process_catalog(data,configuration,parent_job,total_data,processed)
