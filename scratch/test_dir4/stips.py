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
   _t = Task.add_mask(myTask,'*','coords_ready','*')
   return

def write_stips(infile,ra,dec,galradec,filt,ZP_AP,starpre,filedir):
   fileroot=infile
   outfiles = []
        
   outfile = starpre+'_'+filt+'.tbl'
   outfilename = outfile.split('/')[-1]
   flux    = wtips.get_counts(M,ZP_AB)
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
    

def write_stips_input(job_id,event_id,dp_id,filt):
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
   stips_file = read_match(procdp.relativepath[0]+'/'+procdp.filename[0],filternames,myConfig,filt)
   comp_name = 'completed'+myTarget['name']
   options = {comp_name:0}
   _opt = Options(options).create('job',job_id)
   total = len(stips_files)
   _dp = DataProduct(filename=stips_file,relativepath=myConfig.procpath,group='proc',configuration=myConfig).create()
   dpid = int(_dp.dp_id)

def read_match(filepath,cols,myConfig,filt):
   data = np.loadtxt(filepath)
   nstars = len(data[:,0])
   myParams = Parameters.getParam(int(myConfig.config_id))
   area = float(myParams["area"])
   imagesize = float(myParams["imagesize"])
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
      if (col == filt):
         print(filt," is column ",count)
         keepcol = count
   stips_in = []
   M =  data[:,keepcol]
   racent = float(myParams['racent'])
   deccent = float(myParams['deccent'])
   pix = float(myParams['pix'])
   starcoodp = DataProduct.get(myConfig,subtype='star_coord_file') 
   star_coords_path = starcoodp.relativepath + '/' +  starcoodp.filename
   galcoodp = DataProduct.get(myConfig,subtype='gal_coord_file')
   gal_coords_path = galcoodp.relativepath + '/' +  galcoodp.filename
   starcoords = np.loadtxt(star_coord_path) 
   starra = starcoords[:,0]
   stardec = starcoords[:,1]
   #print(len(ra),len(M1))
   filename = filepath.split('/')[-1]
   file1 = filename.split('.')
   file2 = '.'.join(file1[0:len(file1)-1])
   file3 = myConfig.procpath+'/'+file2+str(np.around(hden,decimals=5))+'.'+file1[-1]
   #print("STIPS",file3)
   galradec = np.loadtxt(gal_coords_path)
   stips_list = write_stips(file3,starra,stardec,M,background,galradec,filt)
   del M
   gc.collect()
   _dp = DataProduct(filename=stips_list,relativepath=myConfig.procpath,group='proc',subtype='stips_input',filtername=filt,configuration=myConfig).create()
   return int(_dp.dp_id)

def write_stips(infile,ra,dec,M,background,galradec,filt):
   filternames   = ['Z087','Y106','J129','H158','F184']
   ZP_AB = np.array([26.365,26.357,26.320,26.367,25.913])
   fileroot=infile
   starpre = '_'.join(infile.split('.')[:-1])
   filedir = '/'.join(infile.split('/')[:-1])+'/'
   outfiles = []
   for j,checkfilt in enumerate(filternames):
      if checkfilt==filt:  
         outfile = starpre+'_'+filt+'.tbl'
         outfilename = outfile.split('/')[-1]
         flux    = wtips.get_counts(M,ZP_AB[j])
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
   return mixedfilename
    
def run_stips(dp_id):
   catalogID = dp_id
   catalogDP = DataProduct.get(int(catalogID))
   myTarget = Target.get(int(catalogDP.target_id))
   myConfig = Configuration.get(int(catalogDP.config_id))
   myParams = Parameters.getParam(int(myConfig.config_id))

   fileroot = str(catalogDP.relativepath)
   filename = str(catalogDP.filename) # for example, Mixed_h15_shell_3Mpc_Z.tbl 
   
   filtroot = filename.split('_')[-1].split('.')[0]
   filtername = filtdict[filtroot]
   filename = fileroot+'/'+filename
   seed = np.random.randint(9999)+1000
   with open(filename) as myfile:
      head = [next(myfile) for x in range(3)]
   pos = head[2].split(' ')
   crud,ra = pos[2].split('(')
   dec,crud =  pos[4].split(')')
   print("Running ",filename,ra,dec)
   print("SEED ",seed)
   scene_general = {'ra': myParams['racent'],'dec': myParams['deccent'], 'pa': 0.0, 'seed': seed}
   obs = {'instrument': 'WFI', 'filters': [filtername], 'detectors': 1,'distortion': False, 'oversample': myParams['oversample'], 'pupil_mask': '', 'background': myParams['background_val'], 'observations_id': str(dp_id), 'exptime': myParams['exptime'], 'offsets': [{'offset_id': str(run_id), 'offset_centre': False,'offset_ra': 0.0, 'offset_dec': 0.0, 'offset_pa': 0.0}]}
   obm = ObservationModule(obs, scene_general=scene_general)
   obm.nextObservation()
   source_count_catalogues = obm.addCatalogue(str(filename))
   psf_file = obm.addError()
   fits_file, mosaic_file, params = obm.finalize(mosaic=False)
   #dp_opt = Parameters.getParam(myConfig.config_id) # Attach config params used tp run sim to the DP
   
   _dp = DataProduct(filename=fits_file,relativepath=fileroot,group='proc',subtype='stips_image',filtername=filtername,ra=myParams['racent'], dec=myParams['deccent'],configuration=myConfig).create()


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
      filt = Options.get('event',event_id)['filter']
      stips_list = write_stips_input(job_id,event_id,dp_id,filt)  #Here dp is the match catalog
      run_stips(stips_list)
      parent_job_id = int(event.job_id)
      compname = Options.get('event',event_id)['name']
      update_option = int(Options.get('job',parent_job_id)[compname]) 
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
         newevent = Job.getEvent(thisjob,'stips_done',options={'target_id':tid})
         fire(newevent)           
         logprint(thisconf,thisjob,'stips_done\n')
         logprint(thisconf,thisjob,''.join(["Event= ",str(event.event_id)]))
 
