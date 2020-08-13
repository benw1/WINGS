#! /usr/bin/env python
import gc
import os
import subprocess

import pandas as pd
import wpipe as wp
import numpy as np

if __name__ == '__main__':
    from wingtips import WingTips as wtips
else:
    from wpipe.wingtips import WingTips as wtips


def register(task):
    _temp = task.mask(source='*', name='start', value=task.name)
    _temp = task.mask(source='*', name='split_catalog', value='*')


def split_catalog(job_id,dp_id,detid):
    dp = wp.DataProduct(dp_id)
    cat = dp.filepath + "/" + dp.filename
    ang = orientation*(3.14159/180.0);
    detlocs = np.loadtxt(my_pipe.swroot+'/offsets');
    first,A1,B1 = detlocs[15,:].split(' ')
    outfilelist = []
    ralist = []
    declist = []
   
    foreach line in declocs:
       detnum,Aoff,Boff = line;
       if detnum != detid:
          continue
       Aoff = Aoff - A1;
       Boff = Boff - B1;
       yoff = Boff*np.sin(ang) + Aoff*np.cos(ang);
       xoff = (Aoff*np.sin(ang) - Boff*np.cos(ang))/np.cos(deccent*(3.14159/180.0));
       detracent = racent + xoff;
       detdeccent = deccent + yoff;
       decstr = '%.4f' % detdeccent;
       rastr = '"%.4f' % detracent;
       decstr =~ s/\./p/;
       rastr =~ s/\./p/;
       outfile = fileroot+"/"+rastr+"d"+decstr+"_"+filename;
       ralim1 = detracent-0.14;
       ralim2 = detracent+0.14;
       declim1 = detdeccent-0.1;
       declim2 = detdeccent+0.1;
       my_data = pd.read_csv(cat)
       keep = (my_data.ra > ralim1) & (my_data.ra < ralim2) & (my_data.dec > declim1) & (my_data.dec < declim2)
       my_data[keep].to_csv(outfile, index=False)
       ####NEED TO GENERATE NEW DP HERE###
       ####THEN FIRE EVENT TO PROCESS THIS SPLIT CATALOG####  
         

def read_fixed(filepath, my_config, my_job, racent, deccent):
    data = pd.read_csv(filepath)
    #data.columns = map(str.upper, data.columns)
    nstars = len(data['ra'])
    print(data.columns,"COLS")
    my_params = my_config.parameters
    #area = float(my_params["area"])
    background = my_params["background_dir"]
    #tot_dens = np.float(nstars) / area
    #print("MAX TOTAL DENSITY = ", tot_dens)
    filtsinm = []
    allfilts = ['F062', 'F087', 'F106', 'F129', 'F158', 'F184']
    magni = np.arange(len(data))
    for filt in allfilts:
        try:
            test = data[filt]
            filtsinm = np.append(filtsinm, filt)
            magni = np.vstack((magni, test))
        except KeyError:
            print("NO ", filt, " data found")
    print("FILTERS: ", filtsinm)
    h = data['F158']
    htot_keep = (h > 23.0) & (h < 24.0)
    hkeep = h[htot_keep]
    htot = len(hkeep)
    #hden = np.float(htot) / area
    del h
    #my_job.logprint(''.join(["H(23-24) DENSITY = ", str(hden)]))
    stips_in = []
    ra = data['ra']
    dec = data['dec']
    my_job.logprint(''.join(
        ["MIXMAX COO: ", str(np.min(ra)), " ", str(np.max(ra)), " ", str(np.min(dec)), " ", str(np.max(dec)), "\n"]))
    #racent = float(my_params['racent'])
    #deccent = float(my_params['deccent'])
    if (racent < 0):
        racent = (np.min(ra)+np.max(ra))/2.0
        deccent = (np.min(dec)+np.max(dec))/2.0
        my_params['racent'] = racent
        my_params['deccent'] = deccent 
    try:
        starsonly = int(my_params['starsonly'])
    except:
        starsonly = 0
    magni = magni[1:]
    magni = magni.T
    filename = filepath.split('/')[-1]
    file1 = filename.split('.')
    file2 = '.'.join(file1[0:len(file1) - 1])
    #file3 = my_config.procpath + '/' + file2 + str(np.around(hden, decimals=5)) + '.' + file1[-1]
    file3 = my_config.procpath + '/' + file2 + '.' + file1[-1]
    galradec = getgalradec(file3, ra * 0.0 + racent, dec * 0.0 + deccent, magni, background)
    stips_lists, filters = write_stips(file3, ra, dec, magni, background,
                                       galradec, racent, deccent, starsonly, filtsinm)
    del magni
    gc.collect()
    stips_in = np.append(stips_in, stips_lists)
    return stips_in, filters



def parse_all():
    parser = wp.PARSER
    parser.add_argument('--config', '-c', type=int, dest='config_id',
                        help='Configuration ID')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_all()
    if args.config_id:
        this_config = wp.Configuration(int(args.config_id))
        link_stips_catalogs(this_config)
    else:
        job_id = args.job_id
        this_job = wp.Job(job_id)
        event = this_job.firing_event
        dp_id = event.options['dp_id']
        detid = event.tag
        split_catalog(job_id, dp_id, detid)
