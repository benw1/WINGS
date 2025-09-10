#! /usr/bin/env python
import os 
#import subprocess
from stips.observation_module import ObservationModule
import numpy as np
import wpipe as wp
from astropy.io import fits
import time


filtdict = {'R': 'F062',
            'Z': 'F087',
            'Y': 'F106',
            'J': 'F129',
            'H': 'F158',
            'F': 'F184'}


def register(task):
    _temp = task.mask(source='*', name='start', value=task.name)
    _temp = task.mask(source='*', name='new_stips_catalog', value='*')


def run_stips(event_id, dp_id, ra_dith, dec_dith, detname):
    catalog_dp = wp.DataProduct(dp_id)
    my_config = catalog_dp.config
    my_event = wp.Event(event_id)
    my_params = my_config.parameters
    racent = float(my_params['racent']) + (float(ra_dith) / 3600.0)
    deccent = float(my_params['deccent']) + (float(dec_dith) / 3600.0)
    
    try:
        pa = my_params['orientation']
    except KeyError:
        pa = 0.0

    fileroot = str(catalog_dp.relativepath)
    #print('catalog_dp.procpath = ', catalog_dp.procpath) No attribute procpath
    #fileroot = str(my_config.procpath)
    print('fileroot = ', fileroot)
    filename1 = str(catalog_dp.filename)  # for example, Mixed_h15_shell_3Mpc_F087.tbl
    print('filename1 = ', filename1)
    filtroot = filename1.split('_')[-1].split('.')[0] #filtroot should be filtername ie F087
    print('filtroot = ', filtroot)
    filtername = filtroot
    os.chdir(my_config.procpath) #this is changing the working directory to the proc path
    print('my_config.procpath = ', my_config.procpath)
    filename = fileroot + '/' + filename1 
    filetype = filename1.split('.')[-1]
    print('filetype = ', filetype)
    seed = np.random.randint(9999)+1000


    
    if 'fit' in filetype:
        os.system('cp ' + str(filename) + ' ' + str(my_config.procpath) + '/' + str(filename1))
        print('cp ' + str(filename) + ' ' + str(my_config.procpath) + '/' + str(filename1))
        fileroot = my_config.procpath 
        filename = fileroot + '/' + filename1

        with fits.open(filename) as myfile:
            ra = myfile[1].data['ra'][0]
            dec = myfile[1].data['dec'][0]
    else:
        with open(filename) as myfile:
            head = [next(myfile) for x in range(3)]
        pos = head[2].split(' ')
        crud,ra = pos[2].split('(')
        dec,crud =  pos[4].split(')')

    print("Running ",filename,float(ra),float(dec))
    print("SEED ",seed)
    scene_general = {'ra': float(ra), 'dec': float(dec), 'pa': pa, 'seed': seed}
    obs = {'fast_galaxy': True,'instrument': 'WFI', 'filters': [filtername], 'detectors': 1, 'distortion': False, 'pupil_mask': '', 'background': 'avg',  'observations_id': dp_id, 'exptime': my_params['exptime'], 'residual_readnoise' : False, 'offsets': [{'offset_id': event_id, 'offset_centre': False, 'offset_ra': 0.0, 'offset_dec': 0.0, 'offset_pa': 0.0}]}
    #obm = ObservationModule(obs, scene_general=scene_general, psf_grid_size=int(my_params['psf_grid']), oversample=int(my_params['oversample']), random_seed=seed)
    
    print(obs)
    print(scene_general)
    print('obs = ', [filtername], my_params['detectors'], dp_id, my_params['exptime'], event_id, '\n')
    print('scene_general = ', float(ra), float(dec), pa, seed)
    print('obm = ', int(my_params['psf_grid']), int(my_params['oversample']))
    #print('ObservationModule({}, scene_general={}, psf_grid_size={}, oversample={}, random_seed={})'.format(obs, scene_general, int(my_params['psf_grid']), int(my_params['oversample']), seed))
    
    #obm=ObservationModule(obs, scene_general=scene_general, psf_grid_size=int(my_params['psf_grid']), oversample=int(my_params['oversample']), random_seed=seed)
    print('ObservationModule({}, scene_general={}, psf_grid_size={}, oversample={}, fast_galaxy=True, residual_readnoise=False, residual_cosmic=False, residual_dark=False,random_seed={})'.format(obs, scene_general, int(my_params['psf_grid']), int(my_params['oversample']), seed))
    obm = ObservationModule(obs, scene_general=scene_general, psf_grid_size=int(my_params['psf_grid']), oversample=int(my_params['oversample']), fast_galaxy=True,residual_readnoise=False, residual_cosmic=False, residual_dark=False, random_seed=seed)
    
    print('detector_name in obm default:', obm.instrument.OFFSET_NAMES)
    obm.instrument.OFFSET_NAMES = (detname,)
    print('detector_name in obm changed to:', obm.instrument.OFFSET_NAMES)
    print('ObservationModule({}, scene_general={}, psf_grid_size={}, oversample={}, random_seed={})'.format(obs, scene_general, int(my_params['psf_grid']), int(my_params['oversample']), seed))
    #try:
    #    os.symlink(my_params['psf_cache'],my_config.procpath+"/psf_cache")
    #except:
    #    print("Try-except line 72 failed, config path error")
    print("START obm.nextobservation")
    obm.nextObservation()
    source_count_catalogues = obm.addCatalogue(str(filename))
    print("START psf_file")
    psf_file = obm.addError()
    fits_file, mosaic_file, params = obm.finalize(mosaic=False)
    #detname = filename1.split('_')[1]
    #try:
    #    ndetect = my_params['ndetect']
    #except:
    #    ndetect = 1
    #if ndetect == 1:
    #    this_target = my_config.target
    #    targname = this_target.name
    #    detname = '.'.join(targname.split('.')[:-1])
    detname = my_event.options["detname"]
    this_job.logprint(''.join(["Making DataProduct with DETNAME and confid", detname, str(my_config.config_id), "\n"]))

    _dp = my_config.dataproduct(filename='sim_' + str(dp_id) + '_0.fits', relativepath=my_config.procpath,
                                group='proc', data_type='stips_image', subtype=detname,
                                filtername=filtername, ra=my_params['racent'], dec=my_params['deccent'])
    this_job.logprint(''.join(["Checking: DP ID IS ",str(_dp.dp_id)," and FILENAME is ",_dp.filename]))
    this_job.logprint(''.join(["Checking: DP subtype IS ",str(_dp.subtype)," and config is ",str(my_config.config_id)]))
        #os.system('cp ' + fileroot + '/' + 'sim_' + str(dp_id) + '_0.fits ' + fileroot + '/' + 'sim_' + str(_dp.dp_id) + '_0.fits')
    #print('mv ' + fileroot + '/' + 'sim_' + str(dp_id) + '_0.fits ' + fileroot + '/' + 'sim_' + str(_dp.dp_id) + '_0.fits')
    
    return detname

def get_offsets(obs_ra, obs_dec):
    obs18par = {'fast_galaxy': False,'instrument': 'WFI', 'detectors': 18, 'distortion': False, 'offsets': [{'offset_id': 1, 'offset_centre': False, 'offset_ra': 0.0, 'offset_dec': 0.0, 'offset_pa': 0.0}]}
    residuals = {'residual_flat': False, 'residual_dark': False, 'residual_cosmic': False, 'residual_poisson': True, 'residual_readnoise': False}
    obm18 = ObservationModule(obs18par, ra=obs_ra, dec=obs_dec, residuals=residuals)

    return obm18.instrument.DETECTOR_OFFSETS, obm18.instrument.OFFSET_NAMES


def parse_all():
    parser = wp.PARSER
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_all()
    this_job_id = args.job_id
    this_job = wp.Job(this_job_id)
    this_event = this_job.firing_event
    this_event_id = this_event.event_id
    this_dp_id = this_event.options['dp_id']
    parent_job_id = this_event.parent_job_id
    parent_job = this_event.parent_job
    compname = this_event.options['name']
    ra_dither = this_event.options['ra_dither']
    dec_dither = this_event.options['dec_dither']
    print('event', this_event_id, 'dp', this_dp_id)
    detname = this_event.options['detname']
    print('DETNAME',detname)
    checkname = run_stips(this_event_id, this_dp_id, float(ra_dither), float(dec_dither), detname)
    to_run = this_event.options['to_run']
    catalogID = this_event.options['dp_id']
    catalogDP = wp.DataProduct(catalogID)
    this_conf = catalogDP.config
    this_target = this_conf.target
    #try:
    #    ndetect = my_params['ndetect']
    #except:
    #    this_job.logprint("Couldn't find ndetect parameter, setting to 1")
    #    ndetect = 1
    #if ndetect == 1:
    #    this_job.logprint("ndetect is 1, so setting the detname to the targname")
    #    targname = this_target.name
    #    detname = '.'.join(targname.split('.')[:-1])
    this_job.logprint(''.join(["Grabbing DPS with DETNAME and conf ids of", detname, str(this_conf.config_id),"\n"]))
    print("detname and checkname are ",detname," and ",checkname)
    #if detname == checkname:
    #    print("SAME")
    #else:
    #    print("FAIL, setting detname to checkname")
    #    detname = checkname
    image_dps = wp.DataProduct.select(config_id=str(this_conf.config_id), data_type="stips_image", subtype=detname)
    #image_dps = wp.DataProduct.select(config_id=str(this_conf.config_id), data_type="stips_image")
    update_option = parent_job.options[compname]
    update_option += 1
    this_job.logprint(''.join(["Got ", str(len(image_dps)), " images \n"]))
    this_job.logprint(''.join(["Completed ", str(update_option), " of ", str(to_run), "\n"]))
    if update_option == to_run:
        this_job.logprint(''.join(["Completed ", str(update_option), " and to run is ", str(to_run), " firing event\n"]))
        DP = wp.DataProduct(this_dp_id)
        tid = DP.target_id
        path = this_conf.procpath
        #comp_name = 'completed' + this_target.name
        comp_name = 'completed' + detname
        options = {comp_name: 0}
        this_job.options = options
        total = to_run
        #total = len(image_dps)
        # print(image_dps(0))
        for dps in image_dps:
            #print(dps)
            dpid = dps.dp_id
            st = dps.subtype 
            this_job.logprint(''.join(["ID and subtype ", str(dpid), " and ", str(st), "\n"]))
            new_event = this_job.child_event('stips_done', tag=dpid,
                                             options={'target_id': tid, 'dp_id': dpid, 'submission_type': 'scheduler',
                                                      'name': comp_name, 'to_run': total, 'detname': detname, 'walltime': '2:00:00'})
            this_job.logprint(''.join(["event detname is ", str(detname)]))
            new_event.fire()
            #this_job.logprint('stips_done but not firing any events for now\n')
            this_job.logprint(''.join(["Event= ", str(this_event.event_id)]))
        time.sleep(300)
