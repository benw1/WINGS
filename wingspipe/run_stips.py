#! /usr/bin/env python
import os 
#import subprocess
from stips.observation_module import ObservationModule
import numpy as np
import wpipe as wp
from astropy.io import fits

on_hyak = False
on_pbs = False

filtdict = {'R': 'F062',
            'Z': 'F087',
            'Y': 'F106',
            'J': 'F129',
            'H': 'F158',
            'F': 'F184'}


def register(task):
    _temp = task.mask(source='*', name='start', value=task.name)
    _temp = task.mask(source='*', name='new_stips_catalog', value='*')


def run_stips(event_id, dp_id, ra_dith, dec_dith):
    catalog_dp = wp.DataProduct(dp_id)
    my_config = catalog_dp.config
    my_params = my_config.parameters
    #racent = float(my_params['racent']) + (float(ra_dith) / 3600.0)
    #deccent = float(my_params['deccent']) + (float(dec_dith) / 3600.0)
    try:
        pa = my_params['orientation']
    except KeyError:
        pa = 0.0
    fileroot = str(catalog_dp.relativepath)
    filename1 = str(catalog_dp.filename)  # for example, Mixed_h15_shell_3Mpc_Z.tbl
    filtroot = filename1.split('_')[-1].split('.')[0]
    filtername = filtroot
    os.chdir(my_config.procpath)
    filename = fileroot + '/' + filename1 
    seed = np.random.randint(9999)+1000
    with fits.open(filename) as myfile:
        head = [next(myfile[1].data) for x in range(3)]
        pos = head[2].split(' ')
        crud,ra = pos[2].split('(')
        dec,crud =  pos[4].split(')')
        print("Running ",filename,float(ra),float(dec))
        print("SEED ",seed)
        scene_general = {'ra': float(ra), 'dec': float(dec), 'pa': pa, 'seed': seed}
        obs = {'instrument': 'WFI', 'filters': [filtername], 'detectors': my_params['detectors'], 'distortion': False, 
               'pupil_mask': '', 'background': 'avg', 'observations_id': dp_id, 'exptime': my_params['exptime'], 'offsets': [{'offset_id': event_id, 'offset_centre': False, 'offset_ra': 0.0, 'offset_dec': 0.0, 'offset_pa': 0.0}]}
        obm = ObservationModule(obs, scene_general=scene_general, psf_grid_size=int(my_params['psf_grid']),
                                oversample=int(my_params['oversample']))
        try:
            os.symlink(my_params['psf_cache'],my_config.procpath+"/psf_cache")
        except:
            pass
        obm.nextObservation()
        source_count_catalogues = obm.addCatalogue(str(filename))
        psf_file = obm.addError()
        fits_file, mosaic_file, params = obm.finalize(mosaic=False)
        detname = filename1.split('_')[1]
        _dp = my_config.dataproduct(filename='sim_' + str(dp_id) + '_0.fits', relativepath=fileroot, group='proc', data_type='stips_image', subtype=detname, filtername=filtername, ra=my_params['racent'], dec=my_params['deccent'])
        try:
            ndetect = my_params['ndetect']
        except:
            ndetect = 1
        if ndetect == 1:
            this_target = my_config.target
            targname = this_target.name
            detname = '.'.join(targname.split('.')[:-1])

        this_job.logprint(''.join(["Making DataProduct with DETNAME and confid", detname, str(my_config.config_id), "\n"]))
        _dp = my_config.dataproduct(filename='sim_' + str(dp_id) + '_0.fits', relativepath=fileroot,
                                    group='proc', data_type='stips_image', subtype=detname,
                                    filtername=filtername, ra=my_params['racent'], dec=my_params['deccent'])
    return detname




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
    #gives key error - should this be my_params['ra_dither']?
    dec_dither = this_event.options['dec_dither']
    run_stips(this_event_id, this_dp_id, float(ra_dither), float(dec_dither))
    update_option = parent_job.options[compname]
    update_option += 1
    to_run = this_event.options['to_run']
    catalogID = this_event.options['dp_id']
    detname = this_event.options['detname']
    detname = detname.replace(".cat","")
    print("DETNAME ",detname)
    catalogDP = wp.DataProduct(catalogID)
    this_conf = catalogDP.config
    this_target = this_conf.target
    image_dps = wp.DataProduct.select(config_id=this_conf.config_id, data_type="stips_image", subtype=detname)
    print(''.join(["Completed ", str(update_option), " of ", str(to_run)]))
    #try:
    #    ndetect = my_params['ndetect']
    #except:
    #    ndetect = 1
    #if ndetect == 1:
    #    targname = this_target.name
    #    detname = '.'.join(targname.split('.')[:-1])
    #this_job.logprint(''.join(["Grabbing DPS with DETNAME and conf ids of", detname, str(this_conf.config_id),"\n"]))
    #print("detname and checkname are ",detname," and ",checkname)
    #if detname == checkname:
    #    print("SAME")
    #else:
    #    print("FAIL")
    #image_dps = wp.DataProduct.select(config_id=str(this_conf.config_id), data_type="stips_image", subtype=detname)
    #image_dps = wp.DataProduct.select(config_id=str(this_conf.config_id), data_type="stips_image")
    #this_job.logprint(''.join(["Got ", str(len(image_dps)), " images \n"]))
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
            print(dps)
            dpid = dps.dp_id
            #st = dps.subtype 
            #this_job.logprint(''.join(["ID and subtype ", str(dpid), " and ", str(st), "\n"]))
            new_event = this_job.child_event('stips_done', tag=dpid,
                                             options={'target_id': tid, 'dp_id': dpid, 'submission_type': 'pbs',
                                                      'name': comp_name, 'to_run': total, 'detname': detname})
            this_job.logprint(''.join(["event detname is ", str(detname)]))
            new_event.fire()
            #this_job.logprint('stips_done but not firing any events for now\n')
            this_job.logprint(''.join(["Event= ", str(this_event.event_id)]))
