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
    _temp = task.mask(source='*', name='new_match_catalog', value='*')
    _temp = task.mask(source='*', name='new_fixed_catalog', value='*')
    _temp = task.mask(source='*', name='new_split_catalog', value='*')


def process_fixed_catalog(my_job_id, my_dp_id, racent, deccent):
    my_job = wp.Job(my_job_id)
    catalog_dp = wp.DataProduct(my_dp_id)
    my_target = catalog_dp.target
    # print("NAME",my_target.name)
    my_config = catalog_dp.config
    my_params = my_config.parameters
    fileroot = str(catalog_dp.relativepath)
    filename = str(catalog_dp.filename)  # For example:  'h15.shell.5Mpc.in'
    filepath = fileroot + '/' + filename
    print(fileroot,my_config.procpath)
    if fileroot != my_config.procpath:
       wp.shutil.copy2(filepath, my_config.procpath)
    #
    print("CONFIG PATH ",my_config.procpath)
    print("CONFIG ID ",my_config.config_id)
    print("ra and dec ",racent,deccent)
    fileroot = my_config.procpath + '/'
    procdp = my_config.dataproduct(filename=filename, relativepath=fileroot, group='proc')
    stips_files, filters = read_fixed(procdp.relativepath + '/' + procdp.filename, my_config, my_job, racent, deccent)
    comp_name = 'completed' + my_target.name
    options = {comp_name: 0}
    my_job.options = options
    try:
        ra_dithers = my_params['ra_dithers']
        dec_dithers = my_params['dec_dithers']
        dither_size = my_params['dither_size']
        centdec = my_params['deccent']
        total = len(stips_files) * (int(ra_dithers) * int(dec_dithers))*np.int(my_params['ndetect'])
        i = 0
        for stips_cat in stips_files:
            filtname = filters[i]
            _dp = my_config.dataproduct(filename=stips_cat, relativepath=my_config.procpath, group='proc',
                                        filtername=filtname, subtype='stips_input_catalog')
            stipsfilepath = my_config.procpath + '/' + stips_cat
            print("LINE 49")
            dpid = _dp.dp_id
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
                    _dp = my_config.dataproduct(filename=dithfilename, relativepath=my_config.procpath, group='raw')
                    newdpid = _dp.dp_id
                    eventtag = filtname+'_ra:'+str(k)+'/'+str(ra_dithers)+'_dec:'+str(j)+'/'+str(dec_dithers)
                    #new_event = my_job.child_event('new_stips_catalog', tag=eventtag,
                    #                               options={'dp_id': newdpid, 'to_run': total, 'name': comp_name,'submission_type' : 'pbs',
                    #                                        'ra_dither': ra_dither, 'dec_dither': dec_dither})
                    new_event = my_job.child_event('new_stips_catalog', tag=eventtag,
                                                   options={'dp_id': newdpid, 'to_run': total, 'name': comp_name,
                                                            'ra_dither': ra_dither, 'dec_dither': dec_dither})
                    dithnum += 1
                    my_job.logprint(''.join(["Firing event ", str(new_event.event_id), "  new_stips_catalog"]))
                    new_event.fire()
            i += 1
        my_job.logprint("Dither Success")
        print("Dither Success process")

    except KeyError:
        # except "ksdf":
        my_job.logprint("No Dithers Found")
        print("No Dithers Found")
        print("STIPS",stips_files,filters)
        total = len(stips_files)*np.int(my_params['ndetect'])
        i = 0
        for stips_cat in stips_files:
            filtname = filters[i]
            _dp = my_config.dataproduct(filename=stips_cat, relativepath=my_config.procpath, group='proc',
                                        filtername=filtname, subtype='stips_input_catalog')
            dpid = _dp.dp_id
            new_event = my_job.child_event('new_stips_catalog', tag=filtname,
                                           options={'dp_id': dpid, 'to_run': total, 'name': comp_name, 'submission_type' : 'pbs',
                                                    'ra_dither': 0.0, 'dec_dither': 0.0})
            #new_event = my_job.child_event('new_stips_catalog', tag=filtname,
            #                               options={'dp_id': dpid, 'to_run': total, 'name': comp_name, 
            #                                        'ra_dither': 0.0, 'dec_dither': 0.0})
            my_job.logprint(''.join(["Firing event ", str(new_event.event_id), "  new_stips_catalog"]))
            print(''.join(["Firing event ", str(new_event.event_id), "  new_stips_catalog"]))
            new_event.fire()
            i += 1


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
    if racent == 0.0:
        racent = float(my_params['racent'])
        deccent = float(my_params['deccent'])
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


def process_match_catalog(my_job_id, my_dp_id):
    my_job = wp.Job(my_job_id)
    catalog_dp = wp.DataProduct(my_dp_id)
    my_target = catalog_dp.target
    # print("NAME",my_target.name)
    my_config = catalog_dp.config
    fileroot = str(catalog_dp.relativepath)
    filename = str(catalog_dp.filename)  # For example:  'h15.shell.5Mpc.in'
    filepath = fileroot + '/' + filename
    wp.shutil.copy2(filepath, my_config.procpath)
    #
    fileroot = my_config.procpath + '/'
    procdp = my_config.dataproduct(filename=filename, relativepath=fileroot, group='proc')
    # filternames = my_params[filternames]
    filternames = ['F062', 'F087', 'F106', 'F129', 'F158', 'F184']
    stips_files, filters = read_match(procdp.relativepath + '/' + procdp.filename, filternames, my_config, my_job)
    comp_name = 'completed' + my_target.name
    options = {comp_name: 0}
    my_job.options = options
    total = len(stips_files)
    i = 0
    for stips_cat in stips_files:
        filtname = filters[i]
        _dp = my_config.dataproduct(filename=stips_cat, relativepath=my_config.procpath, group='proc',
                                    filtername=filtname, subtype='stips_input_catalog')
        dpid = _dp.dp_id
        new_event = my_job.child_event('new_stips_catalog', tag=filtname,
                                       options={'dp_id': dpid, 'to_run': total, 'name': comp_name,'submission_type' : 'pbs'})
        my_job.logprint(''.join(["Firing event ", str(new_event.event_id), "  new_stips_catalog"]))
        new_event.fire()
        i += 1


def read_match(filepath, cols, my_config, my_job):
    data = np.loadtxt(filepath)
    np.random.shuffle(data)
    nstars = len(data[:, 0])
    my_params = my_config.parameters
    #area = float(my_params["area"])
    imagesize = float(my_params["imagesize"])
    background = my_params["background_dir"]
    #tot_dens = np.float(nstars) / area
    #print("MAX TOTAL DENSITY = ", tot_dens)
    count = -1
    for col in cols:
        count += 1
        if col == 'F158':
            print("H is column ", count)
            hcol = count
        if col == 'F062':
            print("R is column ", count)
            xcol = count
        if col == 'F106':
            print("Y is column ", count)
            ycol = count
        if col == 'F087':
            print("Z is column ", count)
            zcol = count
        if col == 'F129':
            print("J is column ", count)
            jcol = count
        if col == 'F184':
            print("F is column ", count)
            fcol = count
    h = data[:, hcol]
    htot_keep = (h > 23.0) & (h < 24.0)
    hkeep = h[htot_keep]
    htot = len(hkeep)
    #hden = np.float(htot) / area
    del h
    my_job.logprint(''.join(["H(23-24) DENSITY = ", str(hden)]))
    stips_in = []
    filtsinm = ['F087', 'F106', 'F129', 'F158', 'F184']
    magni1, magni2, magni3, magni4, magni5 = data[:, zcol], data[:, ycol], data[:, jcol], data[:, hcol], data[:, fcol]
    racent = float(my_params['racent'])
    deccent = float(my_params['deccent'])
    pix = float(my_params['pix'])
    try: 
        starsonly = int(my_params['starsonly'])
    except:
        print("Setting starsonly to 0")
        starsonly = 0
    radist = np.abs(1 / ((tot_dens ** 0.5) * np.cos(deccent * 3.14159 / 180.0))) / 3600.0
    decdist = (1 / tot_dens ** 0.5) / 3600.0
    my_job.logprint(''.join(['RA:', str(radist), '\n', 'DEC:', str(decdist), '\n']))
    coordlist = np.arange(np.rint(np.float(len(magni2)) ** 0.5) + 1)
    np.random.shuffle(coordlist)
    # print(radist,decdist)
    ra = 0.0
    dec = 0.0
    for k in range(len(coordlist)):
        ra = np.append(ra,
                       radist * coordlist + racent - (pix * imagesize / (np.cos(deccent * 3.14159 / 180.0) * 7200.0)))
        dec = np.append(dec, np.repeat(decdist * coordlist[k] + deccent - (pix * imagesize / 7200.0), len(coordlist)))
    ra = ra[1:len(magni1) + 1]
    dec = dec[1:len(magni1) + 1]
    my_job.logprint(''.join(
        ["MIXMAX COO: ", str(np.min(ra)), " ", str(np.max(ra)), " ", str(np.min(dec)), " ", str(np.max(dec)), "\n"]))
    magni = np.array([magni1, magni2, magni3, magni4, magni5]).T
    del magni1, magni2, magni3, magni4, magni5
    filename = filepath.split('/')[-1]
    file1 = filename.split('.')
    file2 = '.'.join(file1[0:len(file1) - 1])
    file3 = my_config.procpath + '/' + file2 + str(np.around(hden, decimals=5)) + '.' + file1[-1]
    # print("STIPS",file3)
    galradec = getgalradec(file3, ra * 0.0 + racent, dec * 0.0 + deccent, magni, background)
    stips_lists, filters = write_stips(file3, ra, dec, magni, background,
                                       galradec, racent, deccent, starsonly, filtsinm)
    del magni
    gc.collect()
    stips_in = np.append(stips_in, stips_lists)
    return stips_in, filters


def getgalradec(infile, ra, dec, magni, background):
    filt = 'F087'
    zp_ab = np.array([26.365, 26.357, 26.320, 26.367, 25.913])
    zp_vega = np.array([26.471,25.991,25.858,25.520,25.219,24.588])
    starpre = '.'.join(infile.split('.')[:-1])
    filedir = background + '/'
    outfile = starpre + '_' + filt + '.tbl'
    flux = wtips.get_counts(magni[:, 0], zp_ab[0])
    wtips.from_scratch(flux=flux, ra=ra, dec=dec, outfile=outfile, max_writing_packet=int(np.round(len(flux)/100)))
    stars = wtips([outfile], fast_reader={'chunk_size': 2**20})
    galaxies = wtips([filedir + filt + '.txt'])  # this file will be provided pre-made
    radec = galaxies.random_radec_for(stars)
    return radec


def write_stips(infile, ra, dec, magni, background, galradec, racent, deccent, starsonly, filtsinm):
    filternames = ['F062', 'F087', 'F106', 'F129', 'F158', 'F184']
    zp_ab = np.array([26.5, 26.365, 26.357, 26.320, 26.367, 25.913])
    zp_vega = np.array([26.471,25.991,25.858,25.520,25.219,24.588])
    starpre = '.'.join(infile.split('.')[:-1])
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
        outfile = starpre + '_' + filt + '.tbl'
        outfilename = outfile.split('/')[-1]
        # flux    = wtips.get_counts(magni[:,j],zp_ab[j])
        flux = wtips.get_counts(magni[:, mindex], zp_vega[j])
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


def link_stips_catalogs(my_config):
    my_target = my_config.target
    def_config = my_target.default_conf
    print("CHECK ", def_config.config_id)
    print("DEF CONF ", def_config.config_id)
    my_dp = def_config.dataproducts
    stips_input = my_dp[my_dp.subtype == 'stips_input_catalog']
    my_params = my_config.parameters
    print(stips_input)
    total = len(stips_input)
    my_job = my_config.pipeline.dummy_job
    comp_name = 'completed' + my_target['name']
    options = {comp_name: 0}
    my_job.options = options
    print("DPS0 :", stips_input.dp_id[0])
    for i in range(len(stips_input)):
        print("DP ", stips_input.dp_id[i])
        dp = stips_input[i]
        filename = dp.filename
        filtname = dp.filtername
        path = dp.relativepath
        cat = path + '/' + filename
        newfile = my_config.procpath + '/' + filename
        os.symlink(cat, newfile)
        _dp = my_config.dataproduct(filename=filename, relativepath=my_config.procpath, group='proc',
                                    subtype='stips_input_catalog', filtername=filtname)
        dpid = _dp.dp_id
        try:
            ra_dithers = my_params['ra_dithers']
            dec_dithers = my_params['dec_dithers']
            dither_size = my_params['dither_size']
            centdec = my_params['deccent']
            total = len(stips_input) * (int(ra_dithers) * int(dec_dithers))
            for k in range(int(ra_dithers)):
                ra_dither = dither_size * np.cos(float(centdec) * 3.14159 / 180.0) * int(k)
                for j in range(int(dec_dithers)):
                    dec_dither = dither_size * (int(j))
                    eventtag = filtname+'_ra:'+str(k)+'/'+str(ra_dithers)+'_dec:'+str(j)+'/'+str(dec_dithers)
                    my_event = my_job.child_event('new_stips_catalog', tag=eventtag,
                                                  options={'dp_id': dpid, 'to_run': total, 'name': comp_name,'submission_type':'pbs',
                                                           'ra_dither': ra_dither, 'dec_dither': dec_dither})
                    my_job.logprint(''.join(["Firing event ", str(my_event.event_id), "  new_stips_catalog"]))
                    my_event.fire()

        except KeyError:
            my_event = my_job.child_event('new_stips_catalog', tag=filtname,
                                          options={'dp_id': dpid, 'to_run': total, 'name': comp_name,'submission_type':'pbs'})
            my_job.logprint(''.join(["Firing event ", str(my_event.event_id), "  new_stips_catalog"]))
            my_event.fire()


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
       
        if 'match' in event.name:
            process_match_catalog(job_id, dp_id)
        elif 'fixed' in event.name:
            my_job = wp.Job(job_id)
            catalog_dp = wp.DataProduct(dp_id)
            my_config = catalog_dp.config
            my_params = my_config.parameters
            try:
                ndetect = my_params['ndetect']
            except:
                ndetect = 1
            if ndetect == 1:
                process_fixed_catalog(job_id, dp_id, 0.0, 0.0)
            if ndetect > 1:
                for i in range(ndetect):
                    dpid = dp_id
                    new_event = my_job.child_event('split_catalog', tag=i+1,
                                       options={'dp_id': dpid,'submission_type':'pbs'})
                    my_job.logprint(''.join(["Firing event ", str(new_event.event_id), "  split_catalog"]))
                    new_event.fire()
        elif 'split' in event.name:
            detracent = event.options['racent']
            detdeccent = event.options['deccent']
            my_job = wp.Job(job_id)
            catalog_dp = wp.DataProduct(dp_id)
            my_config = catalog_dp.config
            my_params = my_config.parameters
            process_fixed_catalog(job_id, dp_id, detracent, detdeccent)
