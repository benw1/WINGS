#! /usr/bin/env python
import argparse
import gc
import os
import subprocess

import pandas as pd
import wpipe as wp
from .wingtips import WingTips as wtips
from .wingtips import np


def register(task):
    _temp = task.mask(source='*', name='start', value=task.name)
    _temp = task.mask(source='*', name='new_match_catalog', value='*')
    _temp = task.mask(source='*', name='new_fixed_catalog', value='*')


def process_fixed_catalog(job_id, event_id, dp_id):
    myJob = wp.Job.get(job_id)
    myPipe = wp.Pipeline.get(int(myJob.pipeline_id))

    catalogDP = wp.DataProduct.get(int(dp_id))
    myTarget = wp.Target.get(int(catalogDP.target_id))
    # print("NAME",myTarget['name'])
    myConfig = wp.Configuration.get(int(catalogDP.config_id))
    myParams = wp.Parameters.getParam(int(myConfig.config_id))
    fileroot = str(catalogDP.relativepath)
    filename = str(catalogDP.filename)  # For example:  'h15.shell.5Mpc.in'
    filepath = fileroot + '/' + filename
    _t = subprocess.run(['cp', filepath, myConfig.procpath + '/.'], stdout=subprocess.PIPE)
    #
    fileroot = myConfig.procpath + '/'
    procdp = wp.DataProduct(filename=filename, relativepath=fileroot, group='proc', configuration=myConfig).create()
    stips_files, filters = read_fixed(procdp.relativepath[0] + '/' + procdp.filename[0], myConfig, myJob)
    comp_name = 'completed' + myTarget['name']
    options = {comp_name: 0}
    _opt = wp.Options(options).create('job', job_id)
    centdec = myParams['deccent']
    try:
        ra_dithers = myParams['ra_dithers']
        dec_dithers = myParams['dec_dithers']
        dither_size = myParams['dither_size']
        centdec = myParams['deccent']
        centra = myParams['racent']
        total = len(stips_files) * (int(ra_dithers) * int(dec_dithers))
        i = 0
        for stips_cat in stips_files:
            filtname = filters[i]
            _dp = wp.DataProduct(filename=stips_cat, relativepath=myConfig.procpath, group='proc', filtername=filtname,
                                 subtype='stips_input_catalog', configuration=myConfig).create()
            stipsfilepath = myConfig.procpath + '/' + stips_cat
            print("LINE 49")
            dpid = int(_dp.dp_id)
            print("LINE 51", str(dpid))
            dithnum = 0
            for k in range(int(ra_dithers)):
                print("LINE 53", str(dither_size), str(centdec), str(k))
                ra_dither = float(dither_size) * np.cos(float(centdec) * 3.14159 / 180.0) * int(k)
                print("LINE 57")
                for j in range(int(dec_dithers)):
                    dec_dither = float(dither_size) * (int(j))
                    print("LINE 56")
                    filename = stipsfilepath.split('/')[-1]
                    filtroot = filename.split('_')[-1].split('.')[0]
                    dithfilepath = stipsfilepath.replace(''.join(['_', str(filtroot)]),
                                                         ''.join(['_', str(dithnum), '_', str(filtroot)]))
                    print("DITHFILE ", dithfilepath)
                    subprocess.run(['ln', '-s', stipsfilepath, dithfilepath], stdout=subprocess.PIPE)
                    dithfilename = dithfilepath.split('/')[-1]
                    _dp = wp.DataProduct(filename=dithfilename, relativepath=myConfig.procpath, group='raw',
                                         configuration=myConfig).create()
                    newdpid = int(_dp.dp_id)
                    event = wp.Job.getEvent(myJob, 'new_stips_catalog',
                                            options={'dp_id': newdpid, 'to_run': total, 'name': comp_name,
                                                     'ra_dither': ra_dither, 'dec_dither': dec_dither})
                    dithnum += 1
                    wp.logprint(myConfig, myJob,
                                ''.join(["Firing event ", str(event['event_id'].item()), "  new_stips_catalog"]))
                    wp.fire(event)
            i += 1
        wp.logprint(myConfig, myJob, "Dither Success")
        print("Dither Success process")

    except:
        # except "ksdf":
        wp.logprint(myConfig, myJob, "No Dithers Found")
        print("No Dithers Found")
        total = len(stips_files)
        i = 0
        for stips_cat in stips_files:
            filtname = filters[i]
            _dp = wp.DataProduct(filename=stips_cat, relativepath=myConfig.procpath, group='proc', filtername=filtname,
                                 subtype='stips_input_catalog', configuration=myConfig).create()
            dpid = int(_dp.dp_id)
            event = wp.Job.getEvent(myJob, 'new_stips_catalog',
                                    options={'dp_id': dpid, 'to_run': total, 'name': comp_name, 'ra_dither': 0.0,
                                             'dec_dither': 0.0})
            wp.logprint(myConfig, myJob,
                        ''.join(["Firing event ", str(event['event_id'].item()), "  new_stips_catalog"]))
            wp.fire(event)
            i += 1


def read_fixed(filepath, myConfig, myJob):
    data = pd.read_csv(filepath)
    nstars = len(data['ra'])
    myParams = wp.Parameters.getParam(int(myConfig.config_id))
    area = float(myParams["area"])
    imagesize = float(myParams["imagesize"])
    background = myParams["background_dir"]
    tot_dens = np.float(nstars) / area
    print("MAX TOTAL DENSITY = ", tot_dens)
    count = -1
    filtsinm = []
    allfilts = ['R062', 'Z087', 'Y106', 'J129', 'H158', 'F184']
    M = np.arange(len(data))
    for filt in allfilts:
        try:
            test = data[filt]
            filtsinm = np.append(filtsinm, filt)
            M = np.vstack((M, test))
        except:
            print("NO ", filt, " data found")
    print("FILTERS: ", filtsinm)
    h = data['H158']
    htot_keep = (h > 23.0) & (h < 24.0)
    hkeep = h[htot_keep]
    htot = len(hkeep)
    hden = np.float(htot) / area
    del h
    wp.logprint(myConfig, myJob, ''.join(["H(23-24) DENSITY = ", str(hden)]))

    stips_in = []
    filters = []

    racent = float(myParams['racent'])
    deccent = float(myParams['deccent'])
    pix = float(myParams['pix'])
    starsonly = int(myParams['starsonly'])
    ra = data['ra']
    dec = data['dec']
    wp.logprint(myConfig, myJob, ''.join(
        ["MIXMAX COO: ", str(np.min(ra)), " ", str(np.max(ra)), " ", str(np.min(dec)), " ", str(np.max(dec)), "\n"]))
    M = M[1:]
    M = M.T
    filename = filepath.split('/')[-1]
    file1 = filename.split('.')
    file2 = '.'.join(file1[0:len(file1) - 1])
    file3 = myConfig.procpath + '/' + file2 + str(np.around(hden, decimals=5)) + '.' + file1[-1]
    galradec = getgalradec(file3, ra * 0.0 + racent, dec * 0.0 + deccent, M, background)
    stips_lists, filters = write_stips(file3, ra, dec, M, background, galradec, racent, deccent, starsonly, filtsinm)
    del M
    gc.collect()
    stips_in = np.append(stips_in, stips_lists)
    return stips_in, filters


def process_match_catalog(job_id, event_id, dp_id):
    myJob = wp.Job.get(job_id)
    myPipe = wp.Pipeline.get(int(myJob.pipeline_id))

    catalogDP = wp.DataProduct.get(int(dp_id))
    myTarget = wp.Target.get(int(catalogDP.target_id))
    # print("NAME",myTarget['name'])
    myConfig = wp.Configuration.get(int(catalogDP.config_id))
    myParams = wp.Parameters.getParam(int(myConfig.config_id))
    fileroot = str(catalogDP.relativepath)
    filename = str(catalogDP.filename)  # For example:  'h15.shell.5Mpc.in'
    filepath = fileroot + '/' + filename
    _t = subprocess.run(['cp', filepath, myConfig.procpath + '/.'], stdout=subprocess.PIPE)
    #
    fileroot = myConfig.procpath + '/'
    procdp = wp.DataProduct(filename=filename, relativepath=fileroot, group='proc', configuration=myConfig).create()
    # filternames = myParams[filternames]
    filternames = ['R062', 'Z087', 'Y106', 'J129', 'H158', 'F184']
    stips_files, filters = read_match(procdp.relativepath[0] + '/' + procdp.filename[0], filternames, myConfig, myJob)
    comp_name = 'completed' + myTarget['name']
    options = {comp_name: 0}
    _opt = wp.Options(options).create('job', job_id)
    total = len(stips_files)
    i = 0
    for stips_cat in stips_files:
        filtname = filters[i]
        _dp = wp.DataProduct(filename=stips_cat, relativepath=myConfig.procpath, group='proc', filtername=filtname,
                             subtype='stips_input_catalog', configuration=myConfig).create()
        dpid = int(_dp.dp_id)
        event = wp.Job.getEvent(myJob, 'new_stips_catalog', options={'dp_id': dpid, 'to_run': total, 'name': comp_name})
        wp.logprint(myConfig, myJob, ''.join(["Firing event ", str(event['event_id'].item()), "  new_stips_catalog"]))
        wp.fire(event)
        i += 1


def read_match(filepath, cols, myConfig, myJob):
    data = np.loadtxt(filepath)
    np.random.shuffle(data)
    nstars = len(data[:, 0])
    myParams = wp.Parameters.getParam(int(myConfig.config_id))
    area = float(myParams["area"])
    imagesize = float(myParams["imagesize"])
    background = myParams["background_dir"]
    tot_dens = np.float(nstars) / area
    print("MAX TOTAL DENSITY = ", tot_dens)
    count = -1
    for col in (cols):
        count += 1
        if (col == 'H158'):
            print("H is column ", count)
            hcol = count
        if (col == 'R062'):
            print("R is column ", count)
            xcol = count
        if (col == 'Y106'):
            print("Y is column ", count)
            ycol = count
        if (col == 'Z087'):
            print("Z is column ", count)
            zcol = count
        if (col == 'J129'):
            print("J is column ", count)
            jcol = count
        if (col == 'F184'):
            print("F is column ", count)
            fcol = count
    h = data[:, hcol]
    htot_keep = (h > 23.0) & (h < 24.0)
    hkeep = h[htot_keep]
    htot = len(hkeep)
    hden = np.float(htot) / area
    del h
    wp.logprint(myConfig, myJob, ''.join(["H(23-24) DENSITY = ", str(hden)]))

    stips_in = []
    filters = []

    filtsinm = ['Z087', 'Y106', 'J129', 'H158', 'F184']
    M1, M2, M3, M4, M5 = data[:, zcol], data[:, ycol], data[:, jcol], data[:, hcol], data[:, fcol]
    racent = float(myParams['racent'])
    deccent = float(myParams['deccent'])
    pix = float(myParams['pix'])
    starsonly = int(myParams['starsonly'])
    radist = np.abs(1 / ((tot_dens ** 0.5) * np.cos(deccent * 3.14159 / 180.0))) / 3600.0
    decdist = (1 / tot_dens ** 0.5) / 3600.0
    wp.logprint(myConfig, myJob, ''.join(['RA:', str(radist), '\n', 'DEC:', str(decdist), '\n']))
    coordlist = np.arange(np.rint(np.float(len(M2)) ** 0.5) + 1)
    np.random.shuffle(coordlist)
    # print(radist,decdist)
    ra = 0.0
    dec = 0.0
    for k in range(len(coordlist)):
        ra = np.append(ra,
                       radist * coordlist + racent - (pix * imagesize / (np.cos(deccent * 3.14159 / 180.0) * 7200.0)))
        dec = np.append(dec, np.repeat(decdist * coordlist[k] + deccent - (pix * imagesize / 7200.0), len(coordlist)))
    ra = ra[1:len(M1) + 1]
    dec = dec[1:len(M1) + 1]
    wp.logprint(myConfig, myJob, ''.join(
        ["MIXMAX COO: ", str(np.min(ra)), " ", str(np.max(ra)), " ", str(np.min(dec)), " ", str(np.max(dec)), "\n"]))
    M = np.array([M1, M2, M3, M4, M5]).T
    del M1, M2, M3, M4, M5
    filename = filepath.split('/')[-1]
    file1 = filename.split('.')
    file2 = '.'.join(file1[0:len(file1) - 1])
    file3 = myConfig.procpath + '/' + file2 + str(np.around(hden, decimals=5)) + '.' + file1[-1]
    # print("STIPS",file3)
    galradec = getgalradec(file3, ra * 0.0 + racent, dec * 0.0 + deccent, M, background)
    stips_lists, filters = write_stips(file3, ra, dec, M, background, galradec, racent, deccent, starsonly, filtsinm)
    del M
    gc.collect()
    stips_in = np.append(stips_in, stips_lists)
    return stips_in, filters


def getgalradec(infile, ra, dec, M, background):
    filt = 'Z087'
    ZP_AB = np.array([26.365, 26.357, 26.320, 26.367, 25.913])
    fileroot = infile
    starpre = '_'.join(infile.split('.')[:-1])
    filedir = background + '/'
    outfile = starpre + '_' + filt + '.tbl'
    outfilename = outfile.split('/')[-1]
    flux = wtips.get_counts(M[:, 0], ZP_AB[0])
    wtips.from_scratch(flux=flux, ra=ra, dec=dec, outfile=outfile)
    stars = wtips([outfile])
    galaxies = wtips([filedir + filt + '.txt'])  # this file will be provided pre-made
    radec = galaxies.random_radec_for(stars)
    return radec


def write_stips(infile, ra, dec, M, background, galradec, racent, deccent, starsonly, filtsinm):
    filternames = ['R062', 'Z087', 'Y106', 'J129', 'H158', 'F184']
    ZP_AB = np.array([26.5, 26.365, 26.357, 26.320, 26.367, 25.913])
    fileroot = infile
    starpre = '_'.join(infile.split('.')[:-1])
    filedir = '/'.join(infile.split('/')[:-1]) + '/'
    outfiles = []
    filters = []
    for j, filt in enumerate(filternames):
        checkfilt = 0
        mindex = 0
        for k, filtinm in enumerate(filtsinm):
            if filt in filtinm:
                mindex = k
                checkfilt += 1
        if checkfilt == 0:
            continue
        print("Mindex for ", filt, " is ", mindex)
        outfile = starpre + '_' + filt[0] + '.tbl'
        outfilename = outfile.split('/')[-1]
        # flux    = wtips.get_counts(M[:,j],ZP_AB[j])
        flux = wtips.get_counts(M[:, mindex], ZP_AB[j])
        # This makes a stars only input list
        wtips.from_scratch(flux=flux, ra=ra, dec=dec, outfile=outfile)
        stars = wtips([outfile])
        galaxies = wtips([background + '/' + filt + '.txt'])  # this file will be provided pre-made
        galaxies.flux_to_Sb()  # galaxy flux to surface brightness
        galaxies.replace_radec(galradec)  # distribute galaxies across starfield
        if starsonly < 1:
            stars.merge_with(galaxies)  # merge stars and galaxies list
        outfile = filedir + 'Mixed' + '_' + outfilename
        mixedfilename = 'Mixed' + '_' + outfilename
        stars.write_stips(outfile, ipac=True)
        with open(outfile, 'r+') as f:
            content = f.read()
            f.seek(0, 0)
            f.write('\\type = internal' + '\n' +
                    '\\filter = F' + str(filt[1:]) + '\n' +
                    '\\center = (' + str(racent) +
                    '  ' + str(deccent) + ')\n' +
                    content)
        f.close()
        del stars
        del galaxies
        gc.collect()
        outfiles = np.append(outfiles, mixedfilename)
        filters = np.append(filters, str(filt))
    return outfiles, filters


def link_stips_catalogs(myConfig):
    target_id = myConfig.target_id
    pid = myConfig.pipeline_id
    myTarget = wp.Target.get(int(target_id))
    allConf = wp.Store().select('configurations').loc[pid, target_id, :]
    defConfig1 = allConf[allConf['name'] == 'default']
    print("CHECK ", defConfig1['config_id'].iloc[0])
    print("DEF CONF ", defConfig1['config_id'].iloc[0])
    defConfig = wp.Configuration.get(int(defConfig1['config_id'].iloc[0]))
    myDP = wp.Store().select('data_products').loc[defConfig.pipeline_id, defConfig.target_id, defConfig.config_id, :]
    stips_input = myDP[myDP['subtype'] == 'stips_input_catalog']
    myParams = wp.Parameters.getParam(int(myConfig.config_id))
    print(stips_input)
    total = len(stips_input)
    _job = wp.Job(config=myConfig).create()  # need to create dummy job to keep track of events
    job_id = int(_job.job_id)
    myJob = wp.Job.get(job_id)
    comp_name = 'completed' + myTarget['name']
    options = {comp_name: 0}
    _opt = wp.Options(options).create('job', job_id)
    print("DPS0 :", stips_input['dp_id'].iloc[0])
    for i in range(len(stips_input)):
        print("DP ", stips_input['dp_id'].iloc[i])
        dp = wp.DataProduct.get(int(stips_input['dp_id'].iloc[i]))
        filename = dp['filename']
        filtname = dp['filtername']
        path = dp['relativepath']
        cat = path + '/' + filename
        newfile = myConfig.procpath + '/' + filename
        os.symlink(cat, newfile)
        _dp = wp.DataProduct(filename=filename, relativepath=myConfig.procpath, group='proc',
                             subtype='stips_input_catalog', filtername=filtname, configuration=myConfig).create()
        dpid = int(_dp.dp_id)
        try:
            ra_dithers = myParams['ra_dithers']
            dec_dithers = myParams['dec_dithers']
            dither_size = myParams['dither_size']
            centdec = myParams['deccent']
            centra = myParams['racent']
            total = len(stips_input) * (int(ra_dithers) * int(dec_dithers))
            for k in range(int(ra_dithers)):
                ra_dither = dither_size * np.cos(float(centdec) * 3.14159 / 180.0) * int(k)
                for j in range(int(dec_dithers)):
                    dec_dither = dither_size * (int(j))
                    event = wp.Job.getEvent(myJob, 'new_stips_catalog',
                                            options={'dp_id': dpid, 'to_run': total, 'name': comp_name,
                                                     'ra_dither': ra_dither, 'dec_dither': dec_dither})
                    wp.logprint(myConfig, myJob,
                                ''.join(["Firing event ", str(event['event_id'].item()), "  new_stips_catalog"]))
                    wp.fire(event)

        except:
            event = wp.Job.getEvent(myJob, 'new_stips_catalog',
                                    options={'dp_id': dpid, 'to_run': total, 'name': comp_name})
            wp.logprint(myConfig, myJob,
                        ''.join(["Firing event ", str(event['event_id'].item()), "  new_stips_catalog"]))
            wp.fire(event)


def parse_all():
    parser = wp.PARSER
    parser.add_argument('--R', '-R', dest='REG', action='store_true',
                        help='Specify to Register')
    parser.add_argument('--P', '-p', type=int, dest='PID',
                        help='Pipeline ID')
    parser.add_argument('--C', '-c', type=int, dest='config_id',
                        help='Configuration ID')
    parser.add_argument('--N', '-n', type=str, dest='task_name',
                        help='Name of Task to be Registered')
    parser.add_argument('--E', '-e', type=int, dest='event_id',
                        help='Event ID')
    parser.add_argument('--J', '-j', type=int, dest='job_id',
                        help='Job ID')
    parser.add_argument('--DP', '-dp', type=int, dest='dp_id',
                        help='Dataproduct ID')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_all()
    if args.REG:
        register(wp.SQLPipeline(int(args.PID)).task(name=str(args.task_name)))
    elif args.config_id:
        myConfig = wp.Configuration.get(int(args.config_id))
        link_stips_catalogs(myConfig)
    else:
        job_id = int(args.job_id)
        event_id = int(args.event_id)
        event = wp.Event.get(event_id)
        if 'match' in event['name']:
            dp_id = wp.Options.get('event', event_id)['dp_id']
            process_match_catalog(job_id, event_id, dp_id)
        else:
            dp_id = wp.Options.get('event', event_id)['dp_id']
            process_fixed_catalog(job_id, event_id, dp_id)
