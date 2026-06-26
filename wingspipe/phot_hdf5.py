#! /usr/bin/env python

"""Ingests raw DOLPHOT output (an unzipped .phot file) and converts it
to a dataframe, which is then optionally written to an HDF5 file.
Column names are read in from the accompanying .phot.columns file.

Authors
-------
    Meredith Durbin, February 2018

Use
---
    This script is intended to be executed from the command line as
    such:
    ::
        python ingest_dolphot.py ['filebase'] ['--to_hdf'] ['--full']
    
    Parameters:
    (Required) [filebase] - Path to .phot file, with or without .phot extension.
    (Optional) [--to_hdf] - Whether to write the dataframe to an HDF5 
    file. Default is True.
    (Optional) [--full] - Whether to use the full set of columns (photometry of 
    individual exposures). Default is False.
"""

# Original script by Shellby Albrecht
# Modified and by Myles McKay
import wpipe as wp
import dask.dataframe as dd
import numpy as np
import os
import pandas as pd
import traceback
from astropy.io import fits
from astropy.wcs import WCS
from pathlib import Path



def register(task):
    _temp = task.mask(source="*", name="start", value=task.name)
    _temp = task.mask(source="*", name="culling_done", value="*")


if __name__ == "__main__":
    my_pipe = wp.Pipeline()
    my_job = wp.Job()
    my_job.logprint("processing phot file...")
    my_config = my_job.config
    my_target = my_job.target
    this_event = my_job.firing_event
    my_job.logprint(this_event)
    my_job.logprint(this_event.options)
    my_config = my_job.config
    logpath = my_config.logpath
    procpath = my_config.procpath

# global photometry values
# first 11 columns in raw dolphot output
global_columns = ['ext','chip','x','y','chi_gl','snr_gl','sharp_gl', 
                  'round_gl','majax_gl','crowd_gl','objtype_gl']

# dictionary mapping text in .columns file to column suffix
colname_mappings = {
    'counts,'                            : 'count',
    'sky level,'                         : 'sky',
    'Normalized count rate,'             : 'rate',
    'Normalized count rate uncertainty,' : 'raterr',
    'Instrumental VEGAMAG magnitude,'    : 'vega',
    'Transformed UBVRI magnitude,'       : 'trans',
    'Magnitude uncertainty,'             : 'err',
    'Chi,'                               : 'chi',
    'Signal-to-noise,'                   : 'snr',
    'Sharpness,'                         : 'sharp',
    'Roundness,'                         : 'round',
    'Crowding,'                          : 'crowd',
    'Photometry quality flag,'           : 'flag',
}

def cull_photometry(df, filter_detectors, my_config, snrcut=4.0):
                    #cut_params={'irsharp'   : 0.15, 'ircrowd'   : 2.25,
                    #            'uvissharp' : 0.15, 'uviscrowd' : 1.30,
                    #            'wfcsharp'  : 0.20, 'wfccrowd'  : 2.25}):
    """Add 'ST' and 'GST' flag columns based on stellar parameters.

    TODO:
        - Allow for modification by command line
        - Generalize to more parameters?
        - Allow to interact with perl...?

    Inputs
    ------
    df : DataFrame
        table read in by read_dolphot
    filter_detectors : list of strings
        list of detectors + filters in 'detector-filter' format,
        ex. 'WFC-F814W'
    snrcut : scalar, optional
        minimum signal-to-noise ratio for a star to be flagged as 'ST'
        default: 4.0
    cut_params : dict
        dictionary of parameters to cut on, with keys in '<detector><quantity>'
        format, and scalar values.

    Returns
    -------
    df : DataFrame
        table read in by read_dolphot, with ST and GST columns added
        (name format: '<filter>_(g)st')
    """
    try:
        objcut = my_config.parameters["objcut"]
    except:
        objcut = 3
    try:
        snrcut = my_config.parameters["snrcut"]
    except:
        snrcut = 4.0
        print("No parameter for snrcut, setting to 4")
    for filt in filter_detectors:
        d, f = filt.lower().split('_') # split into detector + filter
        if d=='wfc3' and 'f1' in f:
           d = 'ir'
           try:
               test = my_config.parameters["ir_sharp"]
           except:
               print("No parameter for ir_sharp, setting to 0.15")
               my_config.parameters["ir_sharp"] = 0.15 
           try:
               test = my_config.parameters["ir_crowd"]
           except:
               print("No parameter for ir_crowd, setting to 2.25")
               my_config.parameters["ir_crowd"] = 2.25 
        if d=='wfc3' and 'f1' not in f:
           d = 'uvis'
           try:
               test = my_config.parameters["uvis_sharp"]
           except:
               print("No parameter for uvis_sharp, setting to 0.15")
               my_config.parameters["uvis_sharp"] = 0.15 
           try:
               test = my_config.parameters["uvis_crowd"]
           except:
               print("No parameter for uvis_crowd, setting to 1.3")
               my_config.parameters["uvis_crowd"] = 1.3 
        if d == 'acs':
           d = 'wfc'
           try:
               test = my_config.parameters["wfc_sharp"]
           except:
               print("No parameter for wfc_sharp, setting to 0.2")
               my_config.parameters["wfc_sharp"] = 0.2 
           try:
               test = my_config.parameters["wfc_crowd"]
           except:
               print("No parameter for wfc_crowd, setting to 2.25")
               my_config.parameters["wfc_crowd"] = 2.25 
        if d == 'nircam':
           try:
               test = my_config.parameters["nircam_sharp"]
           except:
               print("No parameter for nircam_sharp, setting to 0.01")
               my_config.parameters["nircam_sharp"] = 0.01 
           try:
               test = my_config.parameters["nircam_crowd"]
           except:
               print("No parameter for nircam_crowd, setting to 0.5")
               my_config.parameters["nircam_crowd"] = 0.5 
        if d == 'roman':
           try:
               test = my_config.parameters["roman_sharp"]
           except:
               print("No parameter for roman_sharp, setting to 0.15")
               my_config.parameters["roman_sharp"] = 0.15
           try:
               test = my_config.parameters["roman_crowd"]
           except:
               print("No parameter for roman_crowd, setting to 0.5")
               my_config.parameters["roman_crowd"] = 0.5 

        try:
            print('Making ST and GST cuts for {}'.format(filt))
            # make boolean arrays for each set of culling parameters
            obj_condition = df.loc[:,'objtype_gl'] < objcut
            snr_condition = df.loc[:,'{}_snr'.format(filt.lower())] > snrcut
            #sharp_condition = df.loc[:,'{}_sharp'.format(f)]**2 < cut_params['{}sharp'.format(d)]
            #crowd_condition = df.loc[:,'{}_crowd'.format(f)] < cut_params['{}crowd'.format(d)]
            sharp_condition = df.loc[:,'{}_sharp'.format(filt.lower())]**2 < my_config.parameters['{}_sharp'.format(d)]
            crowd_condition = df.loc[:,'{}_crowd'.format(filt.lower())] < my_config.parameters['{}_crowd'.format(d)]
            # add st and gst columns
            df.loc[:,'{}_st'.format(filt.lower())] = (obj_condition & snr_condition & sharp_condition).astype(bool)
            df.loc[:,'{}_gst'.format(filt.lower())] = (df['{}_st'.format(filt.lower())] & crowd_condition).astype(bool)
            print('Found {} out of {} stars meeting ST criteria for {}'.format(
                df.loc[:,'{}_st'.format(filt.lower())].sum(), df.shape[0], filt.lower()))
            print('Found {} out of {} stars meeting GST criteria for {}'.format(
                df.loc[:,'{}_gst'.format(filt.lower())].sum(), df.shape[0], filt.lower()))
        except Exception:
            df.loc[:,'{}_st'.format(filt.lower())] = np.nan
            df.loc[:,'{}_gst'.format(filt.lower())] = np.nan
            print('Could not perform culling for {}.\n{}'.format(filt.lower(), traceback.format_exc()))
    return df

def make_header_table(my_config, fitsdir, search_string='*.chip?.fits'):
    """Construct a table of key-value pairs from FITS headers of images
    used in dolphot run. Columns are the set of all keywords that appear
    in any header, and rows are per image.

    Inputs
    ------
    fitsdir : Path 
        directory of FITS files
    search_string : string or regex patter, optional
        string to search for FITS images with. Default is
        '*fl?.chip?.fits'

    Returns
    -------
    df : DataFrame
        A table of header key-value pairs indexed by image name.
    """
    keys = []
    headers = {}
    fitslist = wp.DataProduct.select(
        config_id=my_config.config_id, 
        data_type="image", 
        subtype="SCIENCE_prepped")
    #fitslist = list(fitsdir.glob(search_string))
    if len(fitslist) == 0: # this shouldn't happen
        print('No fits files found in {}!'.format(fitsdir))
        return pd.DataFrame()
    # get headers from each image
    for fitsfile in fitslist:
        fitsname = fitsfile.filename # filename without preceding path
        fitspath = fitsfile.config.procpath + "/" + fitsname
        head = fits.getheader(fitspath, ignore_missing_end=True)
        headers.update({fitsname:head})
        #keys += [k for k in head.keys()]
        for k in head.keys():
            if type(head[k]) is bool:
                head.update({k:str(head[k])})
        keys += [k for k in head.keys()]
    unique_keys = np.unique(keys).tolist()
    remove_keys = ['COMMENT', 'HISTORY', 'FW1ERROR', 'FW2ERROR', 'FWSERROR', 'STATFLAG', 'WFCMPRSD', 'WRTERR', 'BKGDTARG','SUBARRAY', 'BKGSUB', 'COMPRESS', 'CRMASK','']
    for key in remove_keys:
        if key in unique_keys:
            unique_keys.remove(key)
    # construct dataframe
    df = pd.DataFrame(columns=unique_keys)
    for fitsname, head in headers.items():
        row = pd.Series(dict(head.items()))
        df.loc[fitsname.split('.fits')[0]] = row.T
    # I do not know why dask is so bad at mixed types
    # but here is my hacky solution
    try:
        df = df.infer_objects()
    except Exception:
        print("Could not infer objects")
    df_obj = df.select_dtypes(['object'])
    # iterate over columns and force types
    for c in df_obj:
        dtype = pd.api.types.infer_dtype(df[c], skipna=True)
        if dtype == 'string':
            df.loc[:,c] = df.loc[:,c].astype(str)
        elif dtype in ['float','mixed-integer-float']:
            df.loc[:,c] = df.loc[:,c].astype(float)
        elif dtype == 'integer':
            df.loc[:,c] = df.loc[:,c].astype(int)
        elif dtype == 'boolean':
            df.loc[:,c] = df.loc[:,c].astype(bool)
        else:
            print('Unrecognized datatype "{}" for column {}; coercing to string'.format(dtype, c))
            df.loc[:,c] = df.loc[:,c].astype(str)
    return df

def name_columns(colfile):
    """Construct a table of column names for dolphot output, with indices
    corresponding to the column number in dolphot output file.

    Inputs
    ------
    colfile : path
        path to file containing dolphot column descriptions

    Returns
    -------
    df : DataFrame
        A table of column descriptions and their corresponding names.
    filters : list
        List of filters included in output
    """
    df = pd.DataFrame(data=np.genfromtxt(colfile, delimiter='. ', dtype=str),
                          columns=['index','desc']).drop('index', axis=1)
    df = df.assign(colnames='')
    # set first 11 column names
    df.loc[:10,'colnames'] = global_columns
    # set rest of column names
    filters_all = []
    for k, v in colname_mappings.items():
        indices = df[df.desc.str.find(k) != -1].index
        desc_split = df.loc[indices,'desc'].str.split(", ")
        # get indices for columns with combined photometry
        indices_total = indices[desc_split.str.len() == 2]
        # get indices for columns with single-frame photometry
        indices_indiv = indices[desc_split.str.len() > 2]
        filters = desc_split.loc[indices_total].str[-1].str.replace("'",'')
        print("got filters ",filters)
        #filters = "ROMAN_"+desc_split.loc[indices_total].str[-1].str.replace("'",'')
        imgnames = desc_split.loc[indices_indiv].str[1].str.split(' ').str[0]
        filters_all.append(filters.values)
        df.loc[indices_total,'colnames'] = filters.str.lower() + '_' + v.lower()
        df.loc[indices_indiv,'colnames'] = imgnames + '_' + v.lower()
    filters_final = np.unique(np.array(filters_all).ravel())
    if len(filters_final) > 0:
        print('Combined Filters found: {}'.format(filters_final))
    if len(filters_final)==0:

        print("No combined filters found, using images")
        filters_all = []
        for k, v in colname_mappings.items():
            indices = df[df.desc.str.find(k) != -1].index
            desc_split = df.loc[indices,'desc'].str.split(", ")
            # get indices for columns with single-frame photometry
            indices_indiv = indices[desc_split.str.len() > 2]

            filters = "ROMAN_"+desc_split.loc[indices_indiv].str[-2].str.split('_').str[-1]
            filters_all.append(filters.values)
            df.loc[indices_indiv,'colnames'] = filters.str.lower() + '_' + v.lower()
        filters_final = np.unique(np.array(filters_all).ravel())

    print('Filters found: {}'.format(filters_final))
    return df, filters_final

def add_wcs(df, photfile, my_config, detname):
    """Converts x and y columns to world coordinates using drizzled file
    that dolphot uses for astrometry

    Inputs
    ------
    photfile : path
        path to raw dolphot output
    df : DataFrame
        photometry table read in by read_dolphot

    Returns
    -------
    df : DataFrame
        A table of column descriptions and their corresponding names,
        with new 'ra' and 'dec' columns added.
    """
    #drzfiles = list(Path(photfile).parent.glob('*_dr?.chip1.fits'))
    try:
        sing = my_config.parameters['run_single']
    except:
        my_config.parameters['run_single'] = "F"
    if my_config.parameters['run_single'] == "F":
        drzfiles = wp.DataProduct.select(config_id=my_config.config_id, subtype="reference_image")
        # neither of these should happen but just in case
        if len(drzfiles) == 0:
            print('No drizzled files found; looking for reference')
            datadp = wp.DataProduct.select(config_id=str(my_config.config_id), subtype='dolphot_data')
            datadpid = [_dp.dp_id for _dp in datadp]
            dataname = [_dp.filename for _dp in datadp]
            print("DATANAME ",dataname)
            rinds = []
            zinds = []
            yinds = []
            jinds = []
            hinds = []
            finds = []
            kinds = []
            count = 0
            chipname = "chip1"
            for dp in datadp:
                dp_id = dp.dp_id
                filt = str(dp.filtername)
                fname = dp.filename
                if chipname not in fname:
                   thisjob.logprint(''.join([chipname, " not in ",fname,"\n"]))
                   continue
                #print('fname = ', fname)
                if "F062" in filt and detname in fname:
                    rinds.append(dp_id)
                    count += 1
                    print('rinds = ', rinds)
                if "F087" in filt and detname in fname:
                    zinds.append(dp_id)
                    count += 1
                    print('zinds = ', zinds)
                if "F106" in filt and detname in fname:
                    yinds.append(dp_id)
                    count += 1
                    print('yinds = ', yinds)
                if "F129" in filt and detname in fname:
                    jinds.append(dp_id)
                    count += 1
                    print('jinds = ', jinds)
                if "F158" in filt and detname in fname:
                    hinds.append(dp_id)
                    count += 1
                    print('hinds = ', hinds)
                if "F184" in filt and detname in fname:
                    finds.append(dp_id)
                    count += 1
                    print('finds = ', finds)
                if "F213" in filt and detname in fname:
                    finds.append(dp_id)
                    count += 1
                    print('kinds = ', kinds)

            print("INDS ", rinds, zinds, yinds, jinds, hinds, hinds, finds, kinds, datadpid)
            nimg = count
            # my_params = config.parameters
            # refimage = my_params['refimage']  #will make this more flexible later
            refdp = wp.DataProduct(hinds[0]) #hinds[0] is empty because there is no F158
            refimage = str(refdp.filename)
            if "sim" in refimage:
                refimage = target.name + '_' + detname + '_' + str(refdp.dp_id) + '_' + refdp.filtername + ".fits"
            else:
                print("No sim")
            drzfile = my_config.procpath+"/"+str(refdp.filename).strip()
            print('Using {} as astrometric reference'.format(drzfile))
            ra, dec = WCS(drzfile).all_pix2world(df.x.values, df.y.values, 0) #0-based coord system matches dolphot
            print(df.x.values, df.y.values,ra,dec)
            df.insert(4, 'ra', ra)
            df.insert(5, 'dec', dec)
        elif len(drzfiles) > 1:
            my_job.logprint('Multiple drizzled files found: {}'.format(drzfiles))
            ref_detector = detname
            try:
                for cand_ref in drzfiles:
                    if ref_detector in cand_ref.filename:
                        drzfile = my_config.procpath+"/"+str(cand_ref.filename).strip()
                my_job.logprint('Using {} as astrometric reference'.format(drzfile))
            except:
                print('No drizzled files found; looking for reference')
                datadp = wp.DataProduct.select(config_id=str(my_config.config_id), subtype='dolphot_data')
                datadpid = [_dp.dp_id for _dp in datadp]
                dataname = [_dp.filename for _dp in datadp]
                print("DATANAME ",dataname)
                rinds = []
                zinds = []
                yinds = []
                jinds = []
                hinds = []
                finds = []
                kinds = []
                count = 0
                chipname = "chip1"
                for dp in datadp:
                    dp_id = dp.dp_id
                    filt = str(dp.filtername)
                    fname = dp.filename
                    if chipname not in fname:
                       thisjob.logprint(''.join([chipname, " not in ",fname,"\n"]))
                       continue
                    #print('fname = ', fname)
                    if "F062" in filt and detname in fname:
                        rinds.append(dp_id)
                        count += 1
                        print('rinds = ', rinds)
                    if "F087" in filt and detname in fname:
                        zinds.append(dp_id)
                        count += 1
                        print('zinds = ', zinds)
                    if "F106" in filt and detname in fname:
                        yinds.append(dp_id)
                        count += 1
                        print('yinds = ', yinds)
                    if "F129" in filt and detname in fname:
                        jinds.append(dp_id)
                        count += 1
                        print('jinds = ', jinds)
                    if "F158" in filt and detname in fname:
                        hinds.append(dp_id)
                        count += 1
                        print('hinds = ', hinds)
                    if "F184" in filt and detname in fname:
                        finds.append(dp_id)
                        count += 1
                        print('finds = ', finds)
                    if "F213" in filt and detname in fname:
                        finds.append(dp_id)
                        count += 1
                        print('kinds = ', kinds)

                print("INDS ", rinds, zinds, yinds, jinds, hinds, hinds, finds, kinds, datadpid)
                nimg = count
                # my_params = config.parameters
                # refimage = my_params['refimage']  #will make this more flexible later
                refdp = wp.DataProduct(hinds[0]) #hinds[0] is empty because there is no F158
                refimage = str(refdp.filename)
                if "sim" in refimage:
                    refimage = target.name + '_' + detname + '_' + str(refdp.dp_id) + '_' + refdp.filtername + ".fits"
                else:
                    print("No sim")
                drzfile = my_config.procpath+"/"+str(refdp.filename).strip()

            my_job.logprint('Using {} as astrometric reference'.format(drzfile))
            ra, dec = WCS(drzfile).all_pix2world(df.x.values, df.y.values, 0) #0-based coord system matches dolphot
            my_job.logprint(f"{df.x.values}, {df.y.values},{ra},{dec}")
            df.insert(4, 'ra', ra)
            df.insert(5, 'dec', dec)


        else:
            drzfile = my_config.procpath+"/"+str(drzfiles[0].filename).strip()
            print('Using {} as astrometric reference'.format(drzfile))
            ra, dec = WCS(drzfile).all_pix2world(df.x.values, df.y.values, 0) #0-based coord system matches dolphot
            print(df.x.values, df.y.values,ra,dec)
            df.insert(4, 'ra', ra)
            df.insert(5, 'dec', dec)
    if my_config.parameters['run_single'] == "T":
        #drzfile = photfile.split(".param")[0]
        #print('Using {} as astrometric reference'.format(drzfile))
        #ra, dec = WCS(drzfile).all_pix2world(df.x.values, df.y.values, 0)
        #print(df.x.values, df.y.values,ra,dec)
        #df.insert(4, 'ra', ra)
        #df.insert(5, 'dec', dec)
        print('No drizzled files found; skipping RA and Dec')

    return df

def read_dolphot(my_config, photfile, columns_df, filters, detname):
    """Reads in raw dolphot output (.phot file) to a DataFrame with named
    columns, and optionally writes it to a HDF5 file.

    Inputs
    ------
    photile : path
        path to raw dolphot output
    columns_df : DataFrame
        table of column names and indices, created by `name_columns`
    filters : list
        List of filters included in output, also from `name_columns`
    to_hdf : bool, optional
        Whether to write photometry table to HDF5 file. Defaults to False
        in the function definition, but defaults to True when this script
        is called from the command line.
    full : bool, optional
        Whether to include full photometry output in DataFrame. Defaults 
        to False.

    Returns
    -------
    df : DataFrame
        A table of column descriptions and their corresponding names.

    Outputs
    -------
        HDF5 file containing photometry table
    """
    #if not full:
    #    # cut individual chip columns before reading in .phot file
    #columns_df = columns_df[columns_df.colnames.str.find('.chip') == -1]
    try:
         colnames = columns_df.colnames
         print(f"usecols is {columns_df.index.tolist()}")
         usecols = columns_df.index.tolist()
         print(f"usecols is {usecols}")
         # read in dolphot output
         #df = dd.read_csv(photfile, delim_whitespace=True, header=None,
         df = dd.read_csv(photfile, sep='\s+', header=None,
                     usecols=usecols, names=colnames).compute()
         #if to_hdf:
         outfile = photfile + '.hdf5'
         print('Reading in header information from individual images')
         fitsdir = Path(photfile).parent
         header_df = make_header_table(my_config, fitsdir)
         print(f"header_df is {header_df}")
         header_df.to_hdf(outfile, key='fitsinfo', mode='w', format='table',
                          complevel=9, complib='zlib')
         # lambda function to construct detector-filter pairs
         lamfunc = lambda x: '-'.join(x[~(x.str.startswith('CLEAR')|x.str.startswith('nan'))])
         #filter_detectors = header_df.filter(regex='(DETECTOR)|(FILTER)').astype(str).apply(lamfunc, axis=1).unique()
         #cut_params = {'irsharp'   : 0.15, 'ircrowd'   : my, 'uvissharp' : 0.15, 'uviscrowd' : 1.30, 'wfcsharp'  : 0.20, 'wfccrowd'  : 2.25}
         print('Writing photometry to {}'.format(outfile))
         #df0 = df[colnames[colnames.str.find(r'.chip') == -1]]
         df0 = df[colnames[colnames.str.find(r'\ (') == -1]]
         #print("columns are:")
         #print(df0.columns.tolist())  
         #df0 = cull_photometry(df0, filter_detectors,my_config)
         df0 = cull_photometry(df0, filters,my_config)
         #my_config.parameters["det_filters"] = ','.join(filters)
         df0 = add_wcs(df0, photfile, my_config, detname)
         df0.to_hdf(outfile, key='data', mode='a', format='table', 
                    complevel=9, complib='zlib')
         outfile_full = outfile.replace('.hdf5','_full.hdf5')
         os.rename(outfile, outfile_full)
         outfile = outfile_full
         for f in filters:
             print('Writing single-frame photometry table for filter {}'.format(f))
             df.filter(regex='_{}_'.format(f)).to_hdf(outfile_full, key=f, 
                           mode='a', format='table', complevel=9, complib='zlib')
    except:
         columns_df = columns_df[columns_df.colnames.str.find('.chip') == -1]
         colnames = columns_df.colnames
         print(f"usecols is {columns_df.index.tolist()}")
         usecols = columns_df.index.tolist()
         print(f"usecols is {usecols}")
         # read in dolphot output
         df = dd.read_csv(photfile, sep='\s+', header=None,
                     usecols=usecols, names=colnames).compute()
         outfile = photfile + '.hdf5'
         print('Reading in header information from individual images')
         fitsdir = Path(photfile).parent
         header_df = make_header_table(my_config, fitsdir)
         print(f"header_df is {header_df}")
         header_df.to_hdf(outfile, key='fitsinfo', mode='w', format='table',
                          complevel=9, complib='zlib')
         # lambda function to construct detector-filter pairs
         lamfunc = lambda x: '-'.join(x[~(x.str.startswith('CLEAR')|x.str.startswith('nan'))])
         print('Writing photometry to {}'.format(outfile))
         df0 = df[colnames[colnames.str.find(r'\ (') == -1]]
         df0 = cull_photometry(df0, filters,my_config)
         #my_config.parameters["det_filters"] = ','.join(filters)
         df0 = add_wcs(df0, photfile, my_config, detname)
         df0.to_hdf(outfile, key='data', mode='a', format='table',
                    complevel=9, complib='zlib')

    print('Finished writing HDF5 file')
    return outfile

if __name__ == '__main__':
    my_pipe = wp.Pipeline()
    my_job = wp.Job()

    this_event = my_job.firing_event  # parent event obj
    parent_job = this_event.parent_job

    # config_id = this_event.options["config_id"]

# * LOG EVENT INFORMATION
    my_job.logprint(f"This Event: {this_event}")
    my_job.logprint(f"This Event Options: {this_event.options}")

# * Call drizzled image from astrodrozzle dataproduct
    this_dp_id = this_event.options["dp_id"]
    this_dp = wp.DataProduct(int(this_dp_id), group="proc")
    my_job.logprint(
        f"Data Product: {this_dp.filename}\n, Path: {my_config.procpath}\n This DP options{this_dp.options}\n")
    target = this_dp.target

    my_config = this_dp.config
    my_job.logprint(
        f"Target Name: {target.name}\n TargetPath: {target.datapath}\n")


 
    photfile = my_config.procpath+'/'+this_dp.filename #if args.filebase.endswith('.phot') else args.filebase + '.phot'
    #photfile = my_config.procpath + '/' + this_dp.filename #if args.filebase.endswith('.phot') else args.filebase + '.phot'
    detname = this_event.options["detname"]
    colfile = photfile + '.columns'
    my_config.parameters['colfile'] = colfile
    my_job.logprint('Photometry file: {}'.format(photfile))
    my_job.logprint('Columns file: {}'.format(colfile))
    columns_df, filters = name_columns(colfile)
    print("columns_df is ",columns_df)
    
    import time
    t0 = time.time()
    #df = read_dolphot(my_config, photfile, columns_df, filters)
    outfile = read_dolphot(my_config, photfile, columns_df, filters, detname)
    head_tail=os.path.split(outfile)
    outfile_stats = os.stat(outfile)
    size = outfile_stats.st_size / (1024 * 1024 * 1024)
    mem = "50G"
    if size > 3:
        mem = "100G"
    if size > 5:
        mem = "150G"
    if size > 10:
        mem = "200G"
    if size > 15:
        mem = "250G"
    #outfile = this_dp.filename + '_full.hdf5'
    hd5_dp = wp.DataProduct(my_config, filename=head_tail[1], 
                              group="proc", data_type="hdf5 file", subtype="catalog")     
    my_config.parameters['photfile'] = outfile
    t1 = time.time()
    timedelta = t1 - t0
    print('Finished in {}'.format(str(timedelta)) )
    if my_config.parameters["run_single"]=="T":
        tracking_job=wp.Job(this_event.options["tracking_job_id"])
        to_run = this_event.options["to_run"]
        comp_name = 'completed_' + my_target.name
        update_option = tracking_job.options[comp_name]
        update_option += 1
        if update_option == to_run:
            next_event = my_job.child_event(
            name="singles_complete",
            options={"dp_id": hd5_dp.dp_id}
            )  # next event
            next_event.fire()
            time.sleep(150)
 
    else:
        next_event = my_job.child_event(
        name="hdf5_ready",
        options={"dp_id": hd5_dp.dp_id, "memory": mem,"detname": this_event.options["detname"], 'submission_type':'scheduler'}
        )  # next event
        next_event.fire()
        next_event = my_job.child_event(
        name="spatial",
        options={"dp_id": hd5_dp.dp_id, "memory": "200G","detname": this_event.options["detname"], 'submission_type':'scheduler'}
        )  # next event
        next_event.fire()
        next_event = my_job.child_event(
        name="start",value="split_fakestars.py",
        options={"target_id": my_target.target_id,"phot_dp_id":this_dp_id}
        )  # next event
        next_event.fire()
        time.sleep(150)

