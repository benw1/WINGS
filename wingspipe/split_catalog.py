#! /usr/bin/env python
import gc
import os
import subprocess
import time
import vaex
import re
from pathlib import Path
from astropy import coordinates, units
from mhealpy import HealpixMap

import pandas as pd
import wpipe as wp
import numpy as np
import dask.dataframe as dd
from astropy.io import fits

if __name__ == '__main__':
    from wingtips import WingTips as wtips
else:
    from wpipe.wingtips import WingTips as wtips


def register(task):
    _temp = task.mask(source='*', name='start', value=task.name)
    _temp = task.mask(source='*', name='split_catalog', value='*')
    _temp = task.mask(source='*', name='new_healpix_target', value='*')

def rreplace(s, old, new, occurrence):
    li = s.rsplit(old, occurrence)
    return new.join(li)

def get_overlapping_files(my_config,detracor,detdeccor):
    cwd = Path(my_config.parameters["healpix_library_path"])
    print(list(cwd.glob('*.h5'))[0])
    state_file = list(cwd.glob('*.h5'))[0].with_suffix('').with_suffix('.state')
    partial_moc_nuniqs = np.sort([
        int(foo[0])
        for f in cwd.glob(state_file.with_suffix('.*.h5').name)
        if (foo:=re.findall(".*\.(\d+)\.h5", f.name))
        ])
    partial_moc_hp_map = HealpixMap(None, partial_moc_nuniqs, density = True)
    print(detracor,detdeccor)
    detector_vertices = coordinates.ICRS(ra=detracor, dec=detdeccor)
    partial_moc_hp_map.uniq[partial_moc_hp_map.query_polygon(detector_vertices.cartesian.xyz.T.value, inclusive=True)]
    #filelist = np.array(sorted(cwd.glob('*.h5')))[partial_moc_hp_map.query_polygon(detector_vertices.cartesian.xyz.T.value, inclusive=True)]
    filelist = np.array(sorted(cwd.glob('*.h5')))[partial_moc_hp_map.query_polygon(detector_vertices.cartesian.xyz.T.value, inclusive=True)]
    return filelist

def get_detectors(ang):
    det_off = [(-1500,2000,0),(1200,1500,0)]
    det_names = ("SCA02","SCA06")
    return det_off,det_names

def split_healpix(job_id, dp_id):
    dp = wp.DataProduct(dp_id)
    my_job = wp.Job(job_id)
    my_config = dp.config
    my_params = my_config.parameters
    filepath = dp.relativepath+"/"+dp.filename
    data = np.loadtxt(filepath, dtype=str)
    racent = float(data[1])
    deccent = float(data[2])
    my_params["healpix_library_path"] = data[0]
    my_params["racent"] = data[1]
    my_params["deccent"] = data[2]
    try:
        ang = my_params['rotation_angle']
    except:
        ang = 0.0
    detlocs,detnames = get_detectors(ang)
    print(detlocs,detnames,)
    my_detectors = my_params['detectors']
    outfilelist = []
    ralist = []
    declist = []
    detcount = 0
    for detname in detnames:
        if detname not in (my_detectors):
            continue
        offsets = detlocs[detcount]
        #detracent = (racent + offsets[0,detcount]) 
        #detdeccent = (deccent + offsets[1,detcount]) 
        detracent = (racent + offsets[0]/3600.0) 
        detdeccent = (deccent + offsets[1]/3600.0) 

        racor = np.cos(detdeccent * np.pi/180.0)
        detracorners = [detracent - (5.0/(60.0*racor)), detracent - (5.0/(60.0*racor)),detracent + (5.0/(60.0*racor)),detracent + (5.0/(60.0*racor))] * units.deg
        detdeccorners = [detdeccent - 5.0/60.0, detdeccent + (5.0/60.0),detdeccent + (5.0/60.0),detdeccent - (5.0/60.0)] * units.deg
        catfiles = get_overlapping_files(my_config,detracorners,detdeccorners)
        outname = detname + ".filelist"
        output = my_config.confpath + "/" + outname
        with open(output, 'w') as f:
            #print(catfiles, file=f)
            for files in catfiles:
                print(files,file=f)
        _dp = my_config.dataproduct(filename=outname, relativepath=my_config.confpath,subtype="healpix_list", group='conf')
        new_event = my_job.child_event('new_healpix_list', tag=_dp.dp_id, options={'dp_id': _dp.dp_id, 'racent': detracent, 'deccent': detdeccent,
                                                  'submission_type': 'scheduler', 'detname': detname})
        my_job.logprint(''.join(["event detname is ", str(detname)]))
        my_job.logprint(''.join(["Firing event ", str(new_event.event_id), "  new_healpix_list"]))
        new_event.fire()
        detcount += 1
 
def split_catalog(job_id, dp_id, detid):
    dp = wp.DataProduct(dp_id)
    my_job = wp.Job(job_id)
    my_config = dp.config
    my_params = my_config.parameters
    racent = my_params['racent']
    deccent = my_params['deccent']
    my_pipe = my_config.pipeline
    orientation = my_params['orientation']
    backdir = my_params['background_dir']
    ang = orientation * (3.14159 / 180.0)
    #detlocs = np.loadtxt(backdir + '/offsets')
    #first, A1, B1 = detlocs[15, :]
    #print(first, A1, B1)
    #print(A1)
    detlocs,detnames = get_detectors(ang)
    my_detectors = my_params['detectors']
    outfilelist = []
    ralist = []
    declist = []
    detcount = 0
    for detname in detnames:
        if detname not in (my_detectors):
            continue
        offsets = detlocs[detcount]
        #detnum, Aoff, Boff = line
        #print(detnum, detid, Aoff, Boff, ang, np.sin(ang))
        print(detname,offsets,ang, np.sin(ang))
        #if int(detnum) != int(detid):
        #    continue
        #Aoff = Aoff - A1
        #Boff = Boff - B1
        #yoff = Boff * np.sin(ang) + Aoff * np.cos(ang)
        #xoff = (Aoff * np.sin(ang) - Boff * np.cos(ang)) / np.cos(deccent * (3.14159 / 180.0))
        #detracent = racent + xoff
        #detdeccent = deccent + yoff

        detracent = (racent + offsets[0,detcount]) 
        detdeccent = (deccent + offsets[1,detcount]) 
        #decstr = '%.4f' % detdeccent
        #rastr = '"%.4f' % detracent
        #rastr = rastr.lstrip("\"")
        #decstr = decstr.strip()
        #decstr = decstr.replace('.', 'p')
        #rastr = rastr.replace('.', 'p')
        #filename = rastr.strip() + "d" + decstr.strip() + "_" + dp.filename.strip()
        filename = detname + "_" + dp.filename.strip()
        #detname = rastr.strip() + "d" + decstr.strip()
        #detparname = "det" + str(int(detnum)) + "name"
        #det_par = wp.Parameter(my_config, name=detparname)
        #det_par.value = detname
        #my_job.logprint(''.join(["Added detector to configuration ", detparname, detname]))

        outfile = my_config.procpath.strip() + "/" + filename
        print(outfile, my_config.procpath, racent)
        ralim1 = detracent - (0.1 / np.cos(deccent * (3.14159 / 180.0)))
        ralim2 = detracent + (0.1 / np.cos(deccent * (3.14159 / 180.0)))
        declim1 = detdeccent - 0.1
        declim2 = detdeccent + 0.1
        # my_data = pd.read_csv(cat)
        try:
            my_data = dd.read_csv(cat)
            #may need to change to my_datafile = fits.open(cat) and add line my_data=my_datafile[1]
            print(my_data.ra, my_data.dec, ralim1, ralim2, declim1, declim2)
            keep = (my_data.ra > ralim1) & (my_data.ra < ralim2) & (my_data.dec > declim1) & (my_data.dec < declim2)
            # my_data[keep].to_csv(outfile, index=False)
            my_data[keep].to_csv(outfile, index=False, single_file=True)
        except:
            ds = vaex.open(cat)
            ds_small = ds[(ds.ra < ralim2) & (ds.ra > ralim1) & (ds.dec < declim2) & (ds.dec > declim1)]
            ds2 = ds_small['ra','dec','roman_f062','roman_f087','roman_f106','roman_f129','roman_f158','roman_f184']
            ds2.rename('roman_f062','F062')
            ds2.rename('roman_f087','F087')
            ds2.rename('roman_f106','F106')
            ds2.rename('roman_f129','F129')
            ds2.rename('roman_f158','F158')
            ds2.rename('roman_f184','F184')
            outfile = rreplace(outfile, '.hdf5', '.csv', 1)
            filename = rreplace(filename, '.hdf5', '.csv', 1)
            ds2.export_csv(outfile)
        #do we want this to be '.to_csv' or should it be converting to a fits file?
        print("Making dp for ",filename)
        _dp = my_config.dataproduct(filename=filename, relativepath=my_config.procpath, group='proc',
                                    subtype='split_catalog')
        #checksize = len(my_data[keep])
        outfile_stats = os.stat(outfile)
        checksize = outfile_stats.st_size / (1024)
        dpid = _dp.dp_id
        if (checksize < 10000000):
            new_event = my_job.child_event('new_split_catalog', tag=dpid,
                                         options={'dp_id': dpid, 'racent': detracent, 'deccent': detdeccent,
                                                  'submission_type': 'scheduler', 'detname': detname}) 
            my_job.logprint(''.join(["Firing event ", str(new_event.event_id), "  new_split_catalog"]))
            my_job.logprint(''.join(["event detname is ", str(detname)]))
            new_event.fire()
        if (checksize >= 10000000):
            my_job.logprint("Large catalog... waiting 5 minutes to fire event...")
            sleepsec = np.random.randint(500)
            my_job.logprint(str(sleepsec))
            time.sleep(sleepsec)
            new_event = my_job.child_event('new_split_catalog', tag=dpid,
                                         options={'dp_id': dpid, 'racent': detracent, 'deccent': detdeccent,
                                                  'submission_type': 'scheduler', 'detname': detname})
            my_job.logprint(''.join(["event detname is ", str(detname)]))
            my_job.logprint(''.join(["Firing event ", str(new_event.event_id), "  new_split_catalog"]))
            new_event.fire()
        detcount += 1
        time.sleep(15)

def read_fixed(filepath, my_config, my_job, racent, deccent):
    datafile = fits.open(filepath)
    # data.columns = map(str.upper, data.columns)
    nstars = len(datafile[1].data['ra'])
    print(datafile[1].data.columns, "COLS")
    my_params = my_config.parameters
    # area = float(my_params["area"])
    background = my_params["background_dir"]
    # tot_dens = np.float(nstars) / area
    # print("MAX TOTAL DENSITY = ", tot_dens)
    filtsinm = []
    allfilts = ['F062', 'F087', 'F106', 'F129', 'F158', 'F184']
    magni = np.arange(len(datafile.data))
    for filt in allfilts:
        try:
            test = datafile.data[filt]
            filtsinm = np.append(filtsinm, filt)
            magni = np.vstack((magni, test))
        except KeyError:
            print("NO ", filt, " data found")
    print("FILTERS: ", filtsinm)
    h = datafile.data['F158'] #what if the data doesn't have a F158 filter?
    htot_keep = (h > 23.0) & (h < 24.0)
    hkeep = h[htot_keep]
    htot = len(hkeep)
    # hden = np.float(htot) / area
    del h
    # my_job.logprint(''.join(["H(23-24) DENSITY = ", str(hden)]))
    stips_in = []
    ra = datafile.data['ra']
    dec = datafile.data['dec']
    my_job.logprint(''.join(
        ["MIXMAX COO: ", str(np.min(ra)), " ", str(np.max(ra)), " ", str(np.min(dec)), " ", str(np.max(dec)), "\n"]))
    # racent = float(my_params['racent'])
    # deccent = float(my_params['deccent'])
    if (racent < 0):
        racent = (np.min(ra) + np.max(ra)) / 2.0
        deccent = (np.min(dec) + np.max(dec)) / 2.0
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
    # file3 = my_config.procpath + '/' + file2 + str(np.around(hden, decimals=5)) + '.' + file1[-1]
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
        if "healpix" in event.name:
            split_healpix(job_id,dp_id)
        else:
            split_catalog(job_id, dp_id, detid)

