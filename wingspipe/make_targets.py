#! /usr/bin/env python
import numpy as np
import wpipe as wp
from astropy.io import fits

def register(task):
    _temp = task.mask(source='*', name='start', value=task.name)
    _temp = task.mask(source='*', name='__init__', value='*')


def discover_targets(pipeline, this_job):
    for my_input in pipeline.inputs:
        my_target = my_input.target()
        #print("NAME", my_target.name)
        comp_name = 'completed_' + my_target.name
        this_job.options = {comp_name: 0}
        for conf in my_target.configurations:
            target_dps = [dp for dp in conf.dataproducts if dp.group == 'raw']
            for target_dp in target_dps:
                send(target_dp, conf, comp_name, len(target_dps), this_job) # send catalog to next step


def send(dp, conf, comp_name, total, job):
    filepath = dp.relativepath + '/' + dp.filename
    dpid = dp.dp_id
    dpconfig = dp.config
    confid = conf.config_id
    dpconfigid = dpconfig.config_id
    #data = np.loadtxt(filepath, dtype=str, usecols=0, encoding='utf-8')
    #data = fits.open(filepath)
    if '.fit' in dp.filename:
        print("FITS file detected")
        with fits.open(filepath) as data:
        
            if  data[1].header['TFIELDS'] == 13:
                """
                Assumes file is STIPS ready if it has 13 columns - same as STIPS-created catalogues. Note: The file must have the format 'filename_filtername.fits'. The filtername must be of the form 'F087', though the extention may be '.fits' or '.fit'.
                """
                print('File ', filepath, ' has type keyword, assuming STIPS-ready')
                my_target = dp.target
                targname = my_target.name
                detname = '.'.join(targname.split('.')[:-1])
                event = job.child_event('new_stips_catalog', jargs='0', value='0',
                                        options={'dp_id': dpid, 'to_run': total, 'name': comp_name, 
                                                 'ra_dither': 0.0, 'dec_dither': 0.0, 'config_id': confid, 'detname': detname})
                event.fire()
       
            elif 'ra' in str(data[1].data.columns):
                print('File ', filepath, ' has ra keyword, assuming positions defined')
                print('Generating event for dp_id: ', dpid,' and CONF: ', confid)
                eventtag = dpid
                event = job.child_event('new_fixed_catalog', jargs='0', value='0', tag=eventtag,
                                        options={'dp_id': dpid, 'to_run': total, 'name': comp_name, 'config_id': confid})
                print("generated event", event.event_id, "Firing...")
                event.fire()

            else:
                print('File ', filepath, ' does not have type keyword, assuming MATCH output')
                event = job.child_event('new_match_catalog', jargs='0', value='0',
                                    options={'dp_id': dpid, 'to_run': total, 'name': comp_name, 'config_id': confid})
                event.fire()
    
    if '.csv' in dp.filename:
        print("csv file detected")

        with open(filepath) as myfile:
            data = [next(myfile) for x in range(3)]

        if 'type' in str(data[0]):
            print('File ', filepath, ' has type keyword, assuming STIPS-ready')
            my_target = dp.target
            targname = my_target.name
            detname = '.'.join(targname.split('.')[:-1])
            event = job.child_event('new_stips_catalog', jargs='0', value='0',
                                    options={'dp_id': dpid, 'to_run': total, 'name': comp_name,
                                             'ra_dither': 0.0, 'dec_dither': 0.0, 'config_id': confid, 'detname': detname})
            event.fire()

        elif 'ra' in str(data[0]):
            print('File ', filepath, ' has ra keyword, assuming positions defined')
            print('Generating event for dp_id: ', dpid,' and CONF: ', confid)
            eventtag = dpid
            event = job.child_event('new_fixed_catalog', jargs='0', value='0', tag=eventtag,
                                    options={'dp_id': dpid, 'to_run': total, 'name': comp_name, 'config_id': confid})
            print("generated event", event.event_id, "Firing...")
            event.fire()

        else:
            print('File ', filepath, ' does not have type keyword, assuming MATCH output')
            event = job.child_event('new_match_catalog', jargs='0', value='0',
                                    options={'dp_id': dpid, 'to_run': total, 'name': comp_name, 'config_id': confid})
            event.fire()
    if '.hdf5' in dp.filename:
        print("hdf5 file detected")
        eventtag = dpid
        event = job.child_event('new_fixed_catalog', jargs='0', value='0', tag=eventtag,
                options={'dp_id': dpid, 'to_run': total, 'name': comp_name, 'config_id': confid})
        print("generated event", event.event_id, "Firing...")
        event.fire()



    
def parse_all():
    parser = wp.PARSER
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_all()
    discover_targets(wp.Pipeline(), wp.Job(args.job_id))
    # placeholder for additional steps
    print('done')
