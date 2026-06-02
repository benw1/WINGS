#! /usr/bin/env python

"""Ingests raw DOLPHOT output (an unzipped .phot.fake file) and converts it
to a dataframe, which is then optionally written to an HDF5 file.
Output column names are read in from the accompanying .phot.columns file. Input columns must be determined from the parameter file.

Authors
-------
    Meredith Durbin, February 2018

Use
---
    This script is intended to be part of a wpipe pipeline
    It takes the ascii output from a DOLPHOT fake star run and puts it into an hdf5 file with 
    column names that reflect those in the .columns file provided by dolphot
    Parameters:
    (Required) [filebase] - Path to .phot file, with or without .phot extension.
    (Optional) [--to_hdf] - Whether to write the dataframe to an HDF5 
    file. Default is True.
    (Optional) [--full] - Whether to use the full set of columns (photometry of 
    individual exposures). Default is False.
"""

import wpipe as wp
import dask.dataframe as dd
import numpy as np
import os
import pandas as pd
import traceback
from astropy.io import fits
from astropy.wcs import WCS
from pathlib import Path
import shutil



def register(task):
    _temp = task.mask(source="*", name="start", value=task.name)
    _temp = task.mask(source="*", name="fakestars_done", value="*")


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
               test = my_config.parameters["nircam_sharp"]
           except:
               print("No parameter for roman_sharp, setting to 0.15")
               my_config.parameters["roman_sharp"] = 0.15 
               roman_sharp = 0.15 
           try:
               test = my_config.parameters["roman_crowd"]
           except:
               print("No parameter for roman_crowd, setting to 0.5")
               my_config.parameters["roman_crowd"] = 0.5 
               roman_crowd = 0.5

        try:
            print('Making ST and GST cuts for {}'.format(filt))
            # make boolean arrays for each set of culling parameters
            snr_condition = df.loc[:,'{}_snr'.format(filt.lower())] > snrcut
            #sharp_condition = df.loc[:,'{}_sharp'.format(f)]**2 < cut_params['{}sharp'.format(d)]
            #crowd_condition = df.loc[:,'{}_crowd'.format(f)] < cut_params['{}crowd'.format(d)]
            sharp_condition = df.loc[:,'{}_sharp'.format(filt.lower())]**2 < my_config.parameters['{}_sharp'.format(d)]
            crowd_condition = df.loc[:,'{}_crowd'.format(filt.lower())] < my_config.parameters['{}_crowd'.format(d)]
            # add st and gst columns
            df.loc[:,'{}_st'.format(filt.lower())] = (snr_condition & sharp_condition).astype(bool)
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
        keys += [k for k in head.keys()]
    unique_keys = np.unique(keys).tolist()
    remove_keys = ['COMMENT', 'HISTORY', '']
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
def name_columns(param_file,colfile,this_config):
    """Construct a table of column names for dolphot AST output, with indices
    corresponding to the column number in dolphot AST output file.

    Inputs
    ------
    param_file : path
        path to file containing the dolphot parameters
    colfile : path
        path to file containing dolphot column descriptions

    Returns
    -------
    df : DataFrame
        A table of column descriptions and their corresponding names.
    filters : list
        List of filters included in output
    """
    ds = pd.DataFrame(data=np.genfromtxt(colfile, delimiter='. ', dtype=str),
                          columns=['index','desc']).drop('index', axis=1)
    df2 = ds.assign(colnames='')
    df = ds.assign(colnames='')
    # set inputcolumns
    #df.loc[:3,'colnames']=['extin','chipin','xin','yin']
    cnames = ['extin','chipin','xin','yin']
    print("param file ",param_file)
    params = np.genfromtxt(param_file, delimiter=1000,dtype=str)
    colcount = 3
    allfilts = 'xxx'
    for i in range(len(params)):
        imgnum,imname = params[i].split('=')
        if ("chip" in imname and "img" in imgnum and "img0" not in imgnum):
           print(imname.strip()+".fits")
           #print("filename: ",my_config.procpath+"/"+imname.strip()+".fits")
           imdp = wp.DataProduct.select(dpowner_id=my_config.config_id, filename=imname.strip()+".fits") 
           print("len imdp: ",len(imdp))
           imdet = "ROMAN"
           imfilt = imdet+"_"+imdp[0].filtername
           #imfilt = imname.split('_')[2].split('.')[0]
           if imfilt in allfilts:
               colname = [imfilt+str(i)+"_counts_in",imfilt+str(i)+"_mag_in"]
           else:
               allfilts = allfilts+"imfilt"
               colname = [imfilt+"_counts_in",imfilt+"_mag_in"]
           colcount += 1
           #df.loc[colcount:colcount+1,'colnames'] = colname
           #df.[colcount:colcount+1,'colnames'] = colname
           cnames.extend(colname)
           colcount += 1
    colcount += 1
    #print("colnames before global : ", *df['colnames'])
    print("colnames before global : ", *cnames)
    totout = len(df['colnames'])
    cnames.extend(range(totout))
    print("colnames after extend : ", cnames)
    # set first 11 output column names
    #df.loc[colcount:colcount+10,'colnames'] = global_columns
    cnames[colcount:colcount+11]=global_columns
    #print("colnames after global : ", *df['colnames'])
    print("colnames after global : ", cnames)
    #colcount += 10
    # set rest of column names
    print("COLCOUNT ",colcount)
    filters_all = []
    for k, v in colname_mappings.items():
        indices = df2[df2.desc.str.find(k) != -1].index
        desc_split = df2.loc[indices,'desc'].str.split(", ")
        # get indices for columns with combined photometry
        indices_total = indices[desc_split.str.len() == 2] 
        # get indices for columns with single-frame photometry
        indices_indiv = indices[desc_split.str.len() > 2]
        filters = desc_split.loc[indices_total].str[-1].str.replace("'",'')
        imgnames = desc_split.loc[indices_indiv].str[1].str.split(' ').str[0]
        filters_all.append(filters.values)
        #df.loc[indices_total+colcount,'colnames'] = filters.str.lower() + '_' + v.lower()
        #df.loc[indices_indiv+colcount,'colnames'] = imgnames + '_' + v.lower()
        newfiltcols=filters.str.lower() + '_' + v.lower()
        my_list = newfiltcols
        for k2, v2 in my_list.items():
            #print ("k2",k2,"v2",v2)
            #print("CEHCK ", cnames[colcount],cnames[k2+colcount],v2)
            cnames[k2+colcount] = v2
        newindcols = imgnames + '_' + v.lower()
        my_list = newindcols
        for k3, v3 in my_list.items():
            print ("k3",k3,"v3",v3,colcount,len(cnames),k3+colcount)
            #print("CEHCK ", cnames[k3+colcount],v3)
            cnames[k3+colcount] = v3


    filters_final = np.unique(np.array(filters_all).ravel())
    this_config.parameters["det_filters"] = ",".join(filters_final)
    print('Filters found: {}'.format(filters_final))
    print("All columns :",cnames)
    #return df, filters_final
    return cnames, filters_final

def add_wcs(df, my_config):
    """Converts x and y columns to world coordinates using drizzled file
    that dolphot uses for astrometry

    Inputs
    ------
    df : DataFrame
        photometry table read in by read_dolphot

    Returns
    -------
    df : DataFrame
        A table of column descriptions and their corresponding names,
        with new 'ra' and 'dec' columns added.
    """
    drzfiles = wp.DataProduct.select(config_id=my_config.config_id, subtype="reference_prepped") 
    # neither of these should happen but just in case
    if len(drzfiles) == 0:
        print('No drizzled files found; skipping RA and Dec')
    elif len(drzfiles) > 1:
        print('Multiple drizzled files found: {}'.format(drzfiles))
    else:
        drzfile = str(drzfiles[0].filename)
        print('Using {} as astrometric reference'.format(drzfile))
        ra, dec = WCS(drzfile).all_pix2world(df.x.values, df.y.values, 0)
        df.insert(4, 'ra', ra)
        df.insert(5, 'dec', dec)
    return df

def read_dolphot_fake(my_config, fakefile, columns_df, filters):
    """Reads in raw dolphot output (.phot.fake file) to a DataFrame with named
    columns, and optionally writes it to a HDF5 file.

    Inputs
    ------
    my_config : pipeline configuration for this reduction
    fakefile : path
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
        # cut individual chip columns before reading in .phot file
    #    columns_df = columns_df[columns_df.colnames.str.find('.chip') == -1]
    #colnames = columns_df.colnames
    #non_duplicate_indices = colnames.index[~colnames.duplicated(keep='first')].tolist()
    #colnames = colnames[non_duplicate_indices]
    #usecols = list(columns_df.index[non_duplicate_indices])
    #print("indeces ",*non_duplicate_indices)
    #print("usecols ",*usecols)
    colnames1 = columns_df
    colnames = [name for name in colnames1 if '.chip' not in str(name)]

    print("BEFORE UNIQUE: ",colnames)
    unique_values, indices = np.unique(colnames, return_index=True)
    unique_ordered_list = [colnames[i] for i in sorted(indices)]
    colnames = unique_ordered_list
    usecols = sorted(indices)
    # read in dolphot output
    print("fakefile ",fakefile)
    print("colnames ",colnames)
    print("usecols ",usecols)
    df = dd.read_csv(fakefile, delim_whitespace=True, header=None,
                     usecols=usecols, names=colnames,
                     na_values=['-nan']).compute()
                     #na_values=99.999).compute()
    #if to_hdf:
    outfile = fakefile + '.hdf5'
    print('Reading in header information from individual images')
    fitsdir = Path(fakefile).parent
    header_df = make_header_table(my_config, fitsdir)
    header_df.to_hdf(outfile, key='fitsinfo', mode='w', format='table',
                     complevel=9, complib='zlib')
    # lambda function to construct detector-filter pairs
    lamfunc = lambda x: '-'.join(x[~(x.str.startswith('CLEAR')|x.str.startswith('nan'))])
    #filter_detectors = header_df.filter(regex='(DETECTOR)|(FILTER)').astype(str).apply(lamfunc, axis=1).unique()
    #cut_params = {'irsharp'   : 0.15, 'ircrowd'   : my, 'uvissharp' : 0.15, 'uviscrowd' : 1.30, 'wfcsharp'  : 0.20, 'wfccrowd'  : 2.25}
    print('Writing ASTs to {}'.format(outfile))
    #df0 = df[colnames[colnames.str.find(r'.chip') == -1]]
    #df0 = df[colnames[colnames.str.find(r'\ (') == -1]]
    df0 = df[colnames]
    #print("columns are:")
    #print(df0.columns.tolist())
    #df0 = cull_photometry(df0, filter_detectors,my_config)
    df0 = cull_photometry(df0, filters,my_config)
    #df0 = cull_photometry(df0, filters)
    #my_config.parameters["det_filters"] = ','.join(filters)
    df0 = add_wcs(df0, my_config)
    #df0 = add_wcs(df0)
    df0.to_hdf(outfile, key='data', mode='a', format='table',
               complevel=9, complib='zlib')
    #outfile_full = outfile.replace('.hdf5','_full.hdf5')
    #os.rename(outfile, outfile_full)
    for f in filters:
        print('Writing single-frame photometry table for filter {}'.format(f))
        #df.filter(regex='_{}_'.format(f)).to_hdf(outfile_full, key=f,
        df.filter(regex='_{}_'.format(f)).to_hdf(outfile, key=f,
                      mode='a', format='table', complevel=9, complib='zlib')
    print('Finished writing HDF5 file')


if __name__ == '__main__':
    my_pipe = wp.Pipeline()
    my_job = wp.Job()

    this_event = my_job.firing_event  # parent event obj
    parent_job = this_event.parent_job

    # config_id = this_event.options["config_id"]

# * LOG EVENT INFORMATION
    my_job.logprint(f"This Event: {this_event}")
    my_job.logprint(f"This Event Options: {this_event.options}")

    my_target = my_job.target

    my_config = my_job.config
    my_job.logprint(
        f"Target Name: {my_target.name}\n TargetPath: {my_target.datapath}\n")


 
    #fakefile = my_config.procpath+'/'+this_dp.filename #if args.filebase.endswith('.phot') else args.filebase + '.phot'
    fake_dps = wp.DataProduct.select(config_id=str(my_config.config_id), subtype='fake_output')
    output_file = my_config.procpath + "/" + my_target.name + ".phot.fake"
    if os.path.isfile(output_file):
        my_job.logprint(
        f"output file {output_file} already exists... using it...\n")
    else:
        BUFFER_SIZE = 10 * 1024 * 1024 * 1024
        with open(output_file, 'wb') as outfile:
            #for fname in file_names:
            for dps in fake_dps:
                fname = dps.filename
                #with open(fname, 'r') as infile:
                with open(fname, 'rb') as infile:
                    # Read everything and write to the output file
                    #outfile.write(infile.read())
                    shutil.copyfileobj(infile, outfile, BUFFER_SIZE)
    colfile = my_config.parameters['colfile']
    param_file_dp = wp.DataProduct.select(dpowner_id=my_config.config_id, subtype="dolphot_parameters")
    param_file = my_config.confpath + "/" + param_file_dp[0].filename
    my_job.logprint('AST file: {}'.format(output_file))
    my_job.logprint('Columns file: {}'.format(colfile))
    cnames, filters = name_columns(param_file,colfile,my_config)
    
    import time
    t0 = time.time()
    #df = read_dolphot_fake(my_config, fakefile, columns_df, filters)
    df = read_dolphot_fake(my_config,output_file, cnames, filters)
    outfile = output_file + '.hdf5'
    fake_hd5_dp = wp.DataProduct(my_config, filename=outfile, 
                              group="proc", data_type="fake hdf5 file", subtype="fake_catalog")     
    t1 = time.time()
    timedelta = t1 - t0
    print('Finished in {}'.format(str(timedelta)) )
    next_event = my_job.child_event(
    name="fake_hdf5_ready",
    options={"dp_id": fake_hd5_dp.dp_id, "submission_type":"scheduler"}
    )  # next event
    next_event.fire()
    time.sleep(150)


