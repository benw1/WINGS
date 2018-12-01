#! /usr/bin/env python
import argparse,os,subprocess
from wpipe import *

def register(PID,task_name):
   myPipe = Store().select('pipelines').loc[int(PID)]
   myTask = Task(task_name,myPipe).create()
   _t = Task.add_mask(myTask,'*','start',task_name)
   _t = Task.add_mask(myTask,'*','images_prepped','*')
   return

def write_dolphot_pars(target,config,thisjob):
   parfile_name = target['name']+".param"
   parfile_path = config.confpath+'/'+parfile_name
   logprint(config,thisjob,''.join(["Writing dolphot pars now in ",parfile_path,"\n"]))
   myDP = Store().select('data_products').loc[config.pipeline_id,config.target_id,config.config_id,:]
   datadp = myDP[myDP['subtype']=='dolphot_data']
   datadpid = list(set(datadp['dp_id']))
   dataname = set(datadp['filename']) 
   zinds = []
   yinds = []
   jinds = []
   hinds = []
   finds = []
   count = -1
   for dpid in datadpid:
      count += 1
      dp = DataProduct.get(dpid)
      filt = str(dp.filtername)
      if "Z087" in filt:	
        zinds = [zinds,dpid]
      if "Y106" in filt:	
        yinds = [yinds,dpid]
      if "J129" in filt:	
        jinds = [jinds,dpid]
      if "H158" in filt:	
        hinds = [hinds,dpid]
      if "F184" in filt:	
        finds = [finds,dpid]
   zinds = zinds[1:]
   yinds = yinds[1:]
   jinds = jinds[1:]
   hinds = hinds[1:]
   finds = finds[1:]
    
   print("INDS ",zinds,yinds,jinds,hinds,hinds[0],finds,datadpid)
   nimg = len(dataname)
   myParams = Parameters.getParam(int(config.config_id))
   #refimage = myParams['refimage']  #will make this more flexible later
   refdp = DataProduct.get(hinds[0])
   refimage = str(refdp.filename)
   with open(parfile_path, 'w') as d:
      d.write("Nimg = "+str(nimg)+"\n"+
      "img0_file = "+refimage[:-5]+"\n")
      zim = []
      for zind in zinds:
         imdp = DataProduct.get(zind)
         image = str(imdp.filename)
         zim = [zim,image]
      zim = zim[1:]
      yim = []
      for yind in yinds:
         imdp = DataProduct.get(yind)
         image = str(imdp.filename)
         yim = [yim,image]
      yim = yim[1:]
      jim = []
      for jind in jinds:
         imdp = DataProduct.get(jind)
         image = str(imdp.filename)
         jim = [jim,image]
      jim = jim[1:]
      him = []
      for hind in hinds:
         imdp = DataProduct.get(hind)
         image = str(imdp.filename)
         him = [him,image]
      him = him[1:]
      fim = []
      for find in finds:
         fim = []
         imdp = DataProduct.get(find)
         image = str(imdp.filename)
         fim = [fim,image]
      fim = fim[1:]
      zims = set(zim)
      yims = set(yim)
      jims = set(jim)
      hims = set(him)
      fims = set(fim)
      images = [zim,yim,jim,him,fim]
      i=0
      for iimage in images:
         image = iimage[0]
         print("IMAGE ", image)        
         i += 1
         d.write("img"+str(i)+"_file = "+image[:-5]+"\n")
      d.write("img_shift = 0 0\n"+
      "img_xform = 1 0 0\n"+
      "img_RAper = 5\n"+
      "img_RChi  = 2\n"+
      "img_RSky  = 15 35\n"+
      "img_RPSF  = 13\n"+
      "img_apsky = 15 35\n"+

      "RCentroid = 1           #centroid box size (int>0)\n"+
      "SigFind = 3.0           #sigma detection threshold (flt)\n"+
      "SigFindMult = 0.85      #Multiple for quick-and-dirty photometry (flt>0)\n"+
      "SigFinal = 3.5          #sigma output threshold (flt)\n"+
      "MaxIT = 25              #maximum iterations (int>0)\n"+
      "PSFPhot = 1             #photometry type (int/0=aper,1=psf,2=wtd-psf)\n"+
      "PSFPhotIt = 1           #number of iterations in PSF-fitting photometry (int>=0)\n"+
      "FitSky = 2              #fit sky? (int/0=no,1=yes,2=small,3=with-phot)\n"+
      "SkipSky = 2             #spacing for sky measurement (int>0)\n"+
      "SkySig = 2.25           #sigma clipping for sky (flt>=1)\n"+
      "NegSky = 1              #allow negative sky values? (0=no,1=yes)\n"+
      "NoiseMult = 0.1        #noise multiple in imgadd (flt)\n"+
      "FSat = 0.999            #fraction of saturate limit (flt)\n"+
      "PosStep = 0.1           #search step for position iterations (flt)\n"+
      "dPosMax = 2.5           #maximum single-step in position iterations (flt)\n"+
      "RCombine = 1.5          #minimum separation for two stars for cleaning (flt)\n"+
      "SigPSF = 10             #min S/N for psf parameter fits (flt)\n"+
      "UseWCS = 2              #use WCS info in alignment (int 0=no, 1=shift/rotate/scale, 2=full)\n"+
      "Align = 3               #align images? (int 0=no,1=const,2=lin,3=cube)\n"+
      "AlignOnly = 0           #exit after alignment\n"+
      "SubResRef = 1           #subpixel resolution for reference image (int>0)\n"+
      "SecondPass = 3          #second pass finding stars (int 0=no,1=yes)\n"+
      "SearchMode = 1          #algorithm for astrometry (0=max SNR/chi, 1=max SNR)\n"+
      "Force1 = 0              #force type 1/2 (stars)? (int 0=no,1=yes)\n"+
      "PSFres = 0              #make PSF residual image? (int 0=no,1=yes)\n"+
      "ApCor = 1               #find/make aperture corrections? (int 0=no,1=yes)\n"+
      "FakeStars =             #file with fake star input data\n"+
      "FakeOut =               #file with fake star output data (default=phot.fake)\n"+
      "FakeMatch = 3.0         #maximum separation between input and recovered star (flt>0)\n"+
      "FakePSF = 2.0           #assumed PSF FWHM for fake star matching\n"+
      "FakeStarPSF = 1         #use PSF residuals in fake star tests? (int 0=no,1=yes)\n"+
      "RandomFake = 1          #apply Poisson noise to fake stars? (int 0=no,1=yes)\n"+
      "FakePad = 0             #minimum distance of fake star from any chip edge to be used\n"+
      "DiagPlotType = PS       #format to generate diagnostic plots (PNG, GIF, PS)\n"+
      "VerboseData = 1         #to write all displayed numbers to a .data file\n"+
      "ForceSameMag = 0        #force same count rate in images with same filter? (int 0=no, 1=yes)\n"+
      "FlagMask = 4            #photometry quality flags to reject when combining magnitudes\n"+
      "CombineChi = 0          #combined magnitude weights uses chi? (int 0=no, 1=yes)\n"+
      "InterpPSFlib = 0        #interpolate PSF library spatially\n")
   _dp = DataProduct(filename=parfile_name,relativepath=config.confpath,subtype="dolphot_parameters",group='conf',configuration=config).create()
   return _dp
    
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

   return parser.parse_args()

if __name__ == '__main__':
    args = parse_all()
    if args.REG:
       _t = register(args.PID,args.task_name)
    else:
       job_id = int(args.job_id)
       event_id = int(args.event_id)
       event = Event.get(event_id)
       thisjob = Job.get(job_id)
       config = Configuration.get(int(thisjob.config_id))
       tid = config.target_id
       target = Target.get(int(config.target_id))
       paramdp = write_dolphot_pars(target,config,thisjob)
       dpid = int(paramdp.dp_id)
       logprint(config,thisjob,''.join(["Parameter file DPID ",str(dpid),"\n"]))
       newevent = Job.getEvent(thisjob,'parameters_written',options={'target_id':tid, 'dp_id':dpid})
       fire(newevent)
       logprint(config,thisjob,'parameters_written\n')
