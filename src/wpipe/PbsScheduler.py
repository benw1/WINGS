import wpipe
import time
import asyncio
import jinja
import subprocess

class PbsScheduler(object):
   def __init__(self,event,job):
       submit(

   def submit(self,event,job): 
       options = event.options
       try:
          self.cores = options['cores']
       except:
          self.cores = 1
       try: 
          self.memory = options['memory']
       except:
          self.memory = 100
       task = job.task.executable+' -j '+str(job.job_id)
       #Put job into PBS file
       #start async callback of execute method
       
   def execute(self):
       my_pipe = event.pipeline
       pipedir = my_pipe.pipe_root
       #generate task list and pbs script files
       #pbs_command="qsub "+pbs_file      
       subprocess.run(pbs_command,shell=true)
