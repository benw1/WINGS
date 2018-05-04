#! /usr/bin/env python
import time
from stips.observation_module import ObservationModule
filters = ['Z087','Y106','J129','H158','F184']
scene_general = {'ra': 254.56583104, 'dec': 24.10779887, 'pa': 0.0, 'seed': 1234}

tic,toc = time.time(),time.time()

for i,filt in enumerate(filters):
    filename = 'Mixed_h15_shell_5Mpc_'+filt[0]+'.tbl'
    obs = {'instrument': 'WFI', 'filters': [filt], 'detectors': 1, 'distortion': False, 'oversample': 10, 'pupil_mask': '', 'background': 'avg', 'observations_id': int(i+1), 'exptime': 10000, 'offsets': [{'offset_id': 1, 'offset_centre': False, 'offset_ra': 0.0, 'offset_dec': 0.0, 'offset_pa': 0.0}]}
    obm = ObservationModule(obs, scene_general=scene_general)
    obm.nextObservation()
    source_count_catalogues = obm.addCatalogue(filename)
    psf_file = obm.addError()
    fits_file, mosaic_file, params = obm.finalize(mosaic=False)
    print filename
    print fits_file
    print params
    print 'Finished '+filename+' in '+str(time.time()-toc)+' seconds'
    toc = time.time()
else:
    print 'Finished '+str(i+1)+' simulations in '+str(time.time()-tic)+' seconds'
