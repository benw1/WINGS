#! /usr/bin/env python
import os
import graphviz
import pickle
import warnings

import numpy as np
import pandas as pd
import wpipe as wp
from astropy import units as u
from astropy import wcs
from astropy.coordinates import SkyCoord, match_coordinates_sky
from astropy.io import ascii, fits
from astropy.table import Table, vstack
from matplotlib import cm
from matplotlib import pyplot as plt
from scipy.spatial import cKDTree
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.neural_network import MLPClassifier as MLPc
from sklearn.preprocessing import label_binarize, StandardScaler
from sklearn.tree import DecisionTreeClassifier, export_graphviz

warnings.filterwarnings('ignore')


def register(task):
    _temp = task.mask(source='*', name='start', value='*')
    _temp = task.mask(source='*', name='dolphot_done', value='*')


FEAT_NAMES = ['err', 'SNR', 'Sharpness', 'Crowding']

# filter names
FILTERS = np.array(['F062', 'F087', 'F106', 'F129', 'F158', 'F184'])
FILTERSHORT = np.array(['F062', 'F087', 'F106', 'F129', 'F158', 'F184'])

# AB magnitude Zero points
AB_VEGA = np.array([0.2, 0.487, 0.653, 0.958, 1.287, 1.552])

FITS_FILES = ["sim_1_0.fits", "sim_2_0.fits", "sim_3_0.fits",
              "sim_4_0.fits", "sim_5_0.fits"]

SKY_COORD = np.zeros(len(FILTERS))
REF_FITS = int(3)
USE_RADEC = False


def cull_photometry(this_config, this_dp_id):
    phot_dp = wp.DataProduct(this_dp_id)
    phot = phot_dp.filename
    procpath = this_config.procpath
    target = this_config.target
    targname = target.name
    targroot = targname.split('.')[0]
    photpath = procpath + "/" + phot
    print("PHOT PATH: ", photpath, "\n")
    clean_all(photpath, tol=5.0, test_size=0.75, valid_mag=30.0, targroot=targroot)


def clean_all(filename='10_10_phot.txt',
              feature_names=None,
              filters=FILTERS,
              # ab_vega=AB_VEGA,
              fits_files=None,
              ref_fits=REF_FITS,
              sky_coord=SKY_COORD,
              tol=2., test_size=0.1, valid_mag=30.,
              use_radec=USE_RADEC,
              show_plot=False,
              targroot='',
              opt=None):
    """
    Top level wrapper to read data, clean data, train/test/evaluate
    classification model, make figure and display evaluation report

    Calls read_data(), prep_data(), classify() and makePlots().
    """
    if fits_files is None:
        fits_files = FITS_FILES
    if feature_names is None:
        feature_names = FEAT_NAMES
    if opt is None:
        opt = {'evaluate': True,
               'summary': True,
               'plots': True,
               'tree': False,
               'saveClean': True}
    fileroot, filename = os.path.split(filename)
    fileroot += '/'
    filepre = filename.split('.')[0]
    if use_radec:
        sky_coord = [wcs.WCS(fits.open(fileroot + imfile)[1].header) for imfile in fits_files]
    input_data, output_data = read_data(filename=filename,
                                        fileroot=fileroot,
                                        targroot=targroot,
                                        filters=filters)
    in_df, out_df, out_lab = prep_data(input_data, output_data,
                                       use_radec=use_radec,
                                       sky_coord=sky_coord,
                                       filters=filters,
                                       tol=tol,
                                       valid_mag=valid_mag,
                                       ref_fits=ref_fits)
    clf = MLPc(hidden_layer_sizes=(10, 10, 10),
               activation='logistic',
               solver='lbfgs',
               max_iter=20000,
               shuffle=True,
               warm_start=False,
               early_stopping=True)  # , n_iter_no_change=10)
    new_labels = classify(out_df, out_lab,
                          filters=filters,
                          feature_names=feature_names,
                          test_size=test_size,
                          fileroot=fileroot,
                          opt=opt,
                          clf=clf)
    if opt['plots']:
        make_plots(in_df, out_df, new_labels,
                   sky_coord=sky_coord,
                   filters=filters,
                   fileroot=fileroot,
                   nameroot=filepre,
                   tol=tol,
                   use_radec=use_radec,
                   ref_fits=ref_fits,
                   show_plot=show_plot)
    if opt['saveClean']:
        save_cats(input_data, output_data, out_df, new_labels,
                  sky_coord=sky_coord,
                  filters=filters,
                  fileroot=fileroot,
                  nameroot=filepre,
                  tol=tol,
                  use_radec=use_radec,
                  ref_fits=ref_fits,
                  valid_mag=valid_mag)
    return print('\n')


def classify(out_df, out_lab,
             filters=FILTERS,
             feature_names=None,
             test_size=0.9,
             fileroot='',
             opt=None,
             clf=None):
    """
    High level wrapper to build and evaluate classification models
    for all bands and return new labels for the entire dataset.

    For each filter:
    - Extract features and label
    - Split into training and testing dataset
    - Train models, predict label for test set
    - optional: evaluate model performance, make figures,
                save the 'tree', and display report
    - Re-label the entire dataset: for qualitative evaluation

    return an array containing new labels in each filter
    """
    if feature_names is None:
        feature_names = FEAT_NAMES
    if opt is None:
        opt = {'evaluate': True,
               'summary': True,
               'tree': True}
    if clf is None:
        clf = DecisionTreeClassifier()
    new_labels = []
    out_file = fileroot + "_labels.pkl"
    for i, filt in enumerate(filters):
        features = out_df[i][feature_names]
        labels = out_lab[i]
        train_f, test_f, train_l, test_l = train_test_split(features, labels,
                                                            test_size=test_size)
        clf.fit(train_f, train_l)
        pred_l = clf.predict(test_f)
        if opt['evaluate'] | opt['summary']:
            print_report(filt, test_l, pred_l, feature_names,
                         opt['summary'])
        if opt['tree']:
            dot_data = export_graphviz(clf, out_file=None,
                                       leaves_parallel=True,
                                       feature_names=feature_names,
                                       class_names=['other', 'point'],
                                       max_depth=3)
            graph = graphviz.Source(dot_data)
            graph.render(fileroot + filt + '_tree')
        new_labels.append(clf.predict(features))
    print(new_labels)
    pickle.dump(new_labels, open(out_file, 'wb'))
    return new_labels


def read_my_data(fileroot, filenameroot, targroot, filt):
    fits_data = fits.open(fileroot + "Mixed_" + filenameroot + '_' + targroot + '_' + filt + '_observed_SCA01.fits')
    print("ALL has this many tables: ", len(fits_data))
    count = 0
    check = 0
    input_data1 = []
    for table in fits_data:
        hdr = table.header
        print("HEADER: ", repr(hdr))
        if check == 2:
            continue  # Why stop after 2 tables without 'DETECTOR'?
        if 'DETECTOR' in hdr:
            count += 1
            if count == 1:
                input_data1 = [Table.read(table)][0]
                print("INPUT1: ", input_data1)
            else:
                print("COUNT: ", count)
                new = Table.read(table)
                # for p in range(len(new)):
                #    input_data1.add_row(new[p]) 
                input_data1 = vstack([input_data1, new])
        else:
            check += 1
    fits_data.close()
    print("INPUTEND: ", len(input_data1))
    # We do not want the first item? If we do,
    # change to: return input_data1
    return input_data1


def read_data(filename='10_10_phot.txt', fileroot='', targroot='', filters=FILTERS):
    """
    Read in the raw fata files:
    - Input: sythetic photometry file for image generation, IPAC format
    - Output: DOLPHOT measured raw photometry, ASCII format

    Return arrays of AstroPy tables for input and numpy arrays for output
    ordered by corresponding filternames.
    """
    filenameroot = filename.split('.')[0]
    input_data = [read_my_data(fileroot, filenameroot, targroot, filt) for filt in FILTERSHORT]
    output_data = np.loadtxt(fileroot + filename)
    np.random.shuffle(output_data)
    print(input_data[3])
    return input_data, output_data


def prep_data(input_data, output_data, sky_coord=SKY_COORD,
              filters=FILTERS, use_radec=False,
              tol=2., valid_mag=30., ref_fits=0.):
    """
    Prepare the data for classification. The output data is now cleaned
    to exclude low information entries and also labeled based on location
    of detection.

    Return 3 arrays ordered by corresponding filternames:
    - First array for input data in pandas data frames
    - Second array for cleaned output data in pandas data frames
    - Third array for labels of output data in numpy arrays
    """
    nfilt = filters.size
    _xy = output_data[:, 2:4].T
    _count = output_data[:, range(13, 13 + 13 * nfilt, 13)].T
    _vega_mags = output_data[:, range(15, 15 + 13 * nfilt, 13)].T
    _mag_errors = output_data[:, range(17, 17 + 13 * nfilt, 13)].T
    _snr = output_data[:, range(19, 19 + 13 * nfilt, 13)].T
    _sharp = output_data[:, range(20, 20 + 13 * nfilt, 13)].T
    _round = output_data[:, range(21, 21 + 13 * nfilt, 13)].T
    _crowd = output_data[:, range(22, 22 + 13 * nfilt, 13)].T
    in_df, out_df, labels = [], [], []
    for i in range(nfilt):
        in_df.append(pack_input(input_data[i], valid_mag=valid_mag))
        t = validate_output(_mag_errors[i],
                            _count[i], _snr[i],
                            _sharp[i], _round[i],
                            _crowd[i])
        out_df.append(pack_output(_xy, _vega_mags[i], _mag_errors[i],
                                  _count[i], _snr[i], _sharp[i], _round[i],
                                  _crowd[i], t))
        labels.append(label_output(in_df[i], out_df[i],
                                   tol=tol,
                                   valid_mag=valid_mag,
                                   radec={'opt': use_radec,
                                          'wcs1': sky_coord[i],
                                          'wcs2': sky_coord[ref_fits]}))
    return in_df, out_df, labels


def validate_output(err, count, snr, shr, rnd, crd):
    """
    Clean and validate output data
    - Remove measurements with unphysical values, such as negative countrate
    - Remove low information entries, such as magnitude errors >0.5 & SNR <1
    - Remove missing value indicators such as +/- 9.99
    """
    return (err < 0.5) & (count >= 0) & (snr >= 1) & (crd != 9.999) & \
           (shr != 9.999) & (shr != -9.999) & (rnd != 9.999) & (rnd != -9.999)


def scale_features(_df):
    scaler = StandardScaler()
    for i, df in enumerate(_df):
        df['err'] = scaler.fit_transform(df['err'].values.reshape(-1, 1))
        df['Count'] = scaler.fit_transform(df['Count'].values.reshape(-1, 1))
        df['SNR'] = scaler.fit_transform(df['SNR'].values.reshape(-1, 1))
        df['Crowding'] = scaler.fit_transform(df['Crowding'].values.reshape(-1, 1))
        df['Sharpness'] = scaler.fit_transform(df['Sharpness'].values.reshape(-1, 1))
        df['Roundness'] = scaler.fit_transform(df['Roundness'].values.reshape(-1, 1))
        _df[i] = df
    return _df


def pack_input(data, valid_mag=30.):
    """
    return Pandas Dataframes for input AstroPy tables containing
    sources that are brighter than specified magnitude (valid_mag)
    """
    t = data['vegamag'] < valid_mag
    return pd.DataFrame({'x': data['x'][t], 'y': data['y'][t], 'm': data['vegamag'][t], 'type': data['type'][t]})


def pack_output(xy, mags, errs, count, snr, shr, rnd, crd, t):
    """
    return Pandas Dataframes for output numpy arrays including
    all quality parameter
    """
    return pd.DataFrame({'x': xy[0][t], 'y': xy[1][t], 'mag': mags[t], 'err': errs[t],
                         'Count': count[t], 'SNR': snr[t], 'Sharpness': shr[t],
                         'Roundness': rnd[t], 'Crowding': crd[t]})
    # return _df.reindex(np.random.permutation(_df.index))


def label_output(in_df, out_df, tol=2., valid_mag=30., radec=None):
    """
    Label output data entries and return the labels as numpy array.

    Match each remaining output entry with the closest input entry
    within matching radius specified by 'tol' that are brighter than
    specified magnitude (valid_mag).

    Those matched to point source input are labeled '1',
    everything else get '0' label.

    Optionally, use sky_soordinates from the simulated images since
    the images may not be aligned to each other.
    """
    if radec is None:
        radec = {'opt': False, 'wcs1': '', 'wcs2': ''}
    in_x, in_y = in_df['x'].values, in_df['y'].values
    typ_in = in_df['type'].values
    mags = in_df['m'].values
    t = (mags < valid_mag)
    in_x, in_y, typ_in = in_x[t], in_y[t], typ_in[t]
    out_x, out_y = out_df['x'].values, out_df['y'].values
    tmp, typ_out = match_in_out(tol, in_x, in_y, out_x, out_y, typ_in, radec=radec)
    typ_out[typ_out == 'sersic'] = 'other'
    mag_diff = np.zeros(len(in_x))
    mag_diff[tmp != -1] = in_df['m'].values[tmp != -1] - out_df['mag'].values[tmp[tmp != -1]]
    # print(len(typ_out[tmp[tmp!=-1]][np.fabs(mag_diff[tmp!=-1])>0.5]=='point'))
    typ_out[tmp[tmp != -1]][np.fabs(mag_diff[tmp != -1]) > 0.5] = 'other'
    typ_bin = label_binarize(typ_out, classes=['other', 'point'])
    typ_bin = typ_bin.reshape((typ_bin.shape[0],))
    return typ_bin


def input_pair(df, i, j, radec=None):
    """
    Pick sources added in both bands as same object types

    return data dictionary containing the two input magnitudes
    (m1_in, m2_in), coordinates (X, Y) and input source type
    (typ_in)
    """
    if radec is None:
        radec = {'opt': False, 'wcs1': '', 'wcs2': ''}
    m1_in, m2_in, x1, y1, x2, y2 = (df[i]['m'].values, df[j + 1]['m'].values,
                                    df[i]['x'].values, df[i]['y'].values,
                                    df[j + 1]['x'].values, df[j + 1]['y'].values)
    typ1_in, typ2_in = df[i]['type'].values, df[j + 1]['type'].values
    if radec['opt']:
        ra1, dec1 = xy_to_wcs(np.array([x1, y1]).T, radec['wcs1'])
        ra2, dec2 = xy_to_wcs(np.array([x2, y2]).T, radec['wcs2'])
        in12 = match_cats(0.05, ra1, dec1, ra2, dec2)
    else:
        in12 = match_lists(0.1, x1, y1, x2, y2)
    m1_in, x1, y1, typ1_in = m1_in[in12 != -1], x1[in12 != -1], y1[in12 != -1], typ1_in[in12 != -1]
    in12 = in12[in12 != -1]
    m2_in, typ2_in = m2_in[in12], typ2_in[in12]
    tt = typ1_in == typ2_in
    m1_in, m2_in, x, y, typ_in = m1_in[tt], m2_in[tt], x1[tt], y1[tt], typ1_in[tt]
    return dict(zip(['m1_in', 'm2_in', 'X', 'Y', 'typ_in'], [m1_in, m2_in, x, y, typ_in]))


# Recovered source photometry and quality params


def output_pair(df, labels, i, j):
    """
    Pick sources detected in both bands as same object types

    return data dictionary containing the two output magnitudes (mag)
    coordinates (xy), all quality parameters (err,snr,crd,rnd,shr)
    and labels (lbl). Each dictionary item is has two elements for
    two filters (xy has x and y).
    """
    x1, y1, x2, y2 = df[i]['x'].values, df[i]['y'].values, df[j + 1]['x'].values, df[j + 1]['y'].values
    t2 = match_lists(0.1, x1, y1, x2, y2)
    t1 = t2 != -1
    t2 = t2[t2 != -1]
    xy = x1[t1], y1[t1]
    mags = [df[i]['mag'][t1].values, df[j + 1]['mag'][t2].values]
    errs = [df[i]['err'][t1].values, df[j + 1]['err'][t2].values]
    snrs = [df[i]['SNR'][t1].values, df[j + 1]['SNR'][t2].values]
    crds = [df[i]['Crowding'][t1].values, df[j + 1]['Crowding'][t2].values]
    rnds = [df[i]['Roundness'][t1].values, df[j + 1]['Roundness'][t2].values]
    shrs = [df[i]['Sharpness'][t1].values, df[j + 1]['Sharpness'][t2].values]
    lbls = [labels[i][t1], labels[j + 1][t2]]
    nms = ['xy', 'mag', 'err', 'snr', 'crd', 'rnd', 'shr', 'lbl']
    return dict(zip(nms, [xy, mags, errs, snrs, crds, rnds, shrs, lbls]))


def clean_pair(in_pair, out_pair, tol=2., radec=None):
    """
    Re-classify sources detected in both bands as stars. Change detected
    source type from 'star' to 'other' if their location do not match to
    that of a star added in both bands as stars

    return data dictionary containing the two output magnitudes
    (m1, m2), coordinates (X, Y) and output source type (typ_out)
    """
    if radec is None:
        radec = {'opt': False, 'wcs1': '', 'wcs2': ''}
    x1, y1, typ_in = in_pair['X'], in_pair['Y'], in_pair['typ_in']
    x2, y2 = out_pair['xy'][0], out_pair['xy'][1]
    m1_out, m2_out = out_pair['mag'][0], out_pair['mag'][1]
    t1, t2 = out_pair['lbl'][0], out_pair['lbl'][1]
    t = (t1 == 1) & (t2 == 1)
    x2, y2, m1_out, m2_out = x2[t], y2[t], m1_out[t], m2_out[t]
    tmp, typ_out = match_in_out(tol, x1, y1, x2, y2, typ_in, radec=radec)
    return dict(zip(['m1', 'm2', 'x', 'y', 'typ_out'], [m1_out, m2_out, x2, y2, typ_out]))


def save_cats(in_dat, out_dat, out_df, labels,
              sky_coord=SKY_COORD, fileroot='', nameroot='',
              filters=FILTERS, tol=2., ref_fits=0.,
              use_radec=False, valid_mag=30.):
    i = -1
    flags = []
    _X, _Y = out_dat[:, 2].T, out_dat[:, 3].T
    for data, df, label, filt in zip(in_dat, out_df, labels, filters):
        i += 1
        t = data['vegamag'] < valid_mag
        _df1 = pd.DataFrame({'x': data['x'], 'y': data['y'], 'mag': data['vegamag']})
        _df2 = df[label == 1]

        x1, y1 = _df1['x'].values, _df1['y'].values
        x2, y2 = _df2['x'].values, _df2['y'].values
        if use_radec:
            ra1, dec1 = xy_to_wcs(np.array([x1, y1]).T, sky_coord[i])
            ra2, dec2 = xy_to_wcs(np.array([x2, y2]).T, sky_coord[ref_fits])
            in1 = match_cats(tol * 0.11, ra1, dec1, ra2, dec2)
            in2 = match_cats(tol * 0.11, ra2, dec2, ra1[t], dec1[t])
        else:
            in1 = match_lists(tol, x1, y1, x2, y2)
            in2 = match_lists(tol, x2, y2, x1[t], y1[t])
        # Extend input list with recovered mag
        re_mag = np.repeat(99.99, len(x1))
        re_x = np.repeat(99.99, len(x1))
        re_y = np.repeat(99.99, len(x1))
        _t = (in1 != -1) & t
        re_mag[_t] = _df2['mag'].values[in1[_t]]
        re_x[_t] = x2[in1[_t]]
        re_y[_t] = y2[in1[_t]]
        data['recovmag'] = re_mag
        data['recov_x'] = re_x
        data['recov_y'] = re_y
        ascii.write(data, fileroot + nameroot + '_' + str(filt) + '_recov_input.txt', format='ipac')
        # Extend output list with input mag
        inmag = np.repeat(99.99, len(x2))
        _t = in2 != -1
        inmag[_t] = _df1['mag'].values[t][in2[_t]]
        _df2['inputmag'] = inmag
        _df2[['x', 'y', 'mag', 'err', 'inputmag', 'Count', 'Crowding', 'Roundness', 'SNR', 'Sharpness']]. \
            to_csv(fileroot + nameroot + '_' + str(filt) + '_clean.csv', index=False)
        # Make shorter recovered phot file keeping sources kept in at least one filter
        in1 = match_lists(0.1, _X, _Y, x2, y2)
        flag = np.zeros(len(_X))
        flag[in1 != -1] = 1
        flags.append(flag)
    flag = np.sum(flags, axis=0)
    idx = np.arange(len(flag))
    idx = idx[flag != 0]
    new_dat = out_dat[idx, :]
    return np.savetxt(fileroot + nameroot + '_' + 'Clean_Catalog.phot', new_dat, fmt='%10.7e')


def match_lists(tol, x1, y1, x2, y2):
    """
    Match X and Y coordinates using cKDTree
    return index of 2nd list at coresponding position in the 1st
    return -1 if no match is found within matching radius (tol)
    """
    d1 = np.empty((x1.size, 2))
    d2 = np.empty((x2.size, 2))
    d1[:, 0], d1[:, 1] = x1, y1
    d2[:, 0], d2[:, 1] = x2, y2
    t = cKDTree(d2)
    tmp, in1 = t.query(d1, distance_upper_bound=tol)
    in1[in1 == x2.size] = -1
    return in1


def match_cats(tol, ra1, dec1, ra2, dec2):
    """
    Match astronomical coordinates using SkyCoord
    return index of 2nd list at coresponding position in the 1st
    return -1 if no match is found within matching radius (tol)
    """
    c1 = SkyCoord(ra=ra1 * u.degree, dec=dec1 * u.degree)
    c2 = SkyCoord(ra=ra2 * u.degree, dec=dec2 * u.degree)
    in1, sep, tmp = match_coordinates_sky(c1, c2, storekdtree=False)
    sep = sep.to(u.arcsec)
    in1[in1 == ra2.size] = -1
    in1[sep > tol * u.arcsec] = -1
    return in1


def match_in_out(tol, in_x, in_y, out_x, out_y, typ_in, radec=None):
    """
    Match input coordnates to recovered coordinates picking the
    closest matched item.

    return index of output entry at coresponding position in the
    input list and source type of the matching input

    return -1 as the index if no match is found and source type
    as 'other' (not point source)
    """
    if radec is None:
        radec = {'opt': False, 'wcs1': '', 'wcs2': ''}
    if radec['opt']:
        ra1, dec1 = xy_to_wcs(np.array([in_x, in_y]).T, radec['wcs1'])
        ra2, dec2 = xy_to_wcs(np.array([out_x, out_y]).T, radec['wcs2'])
        in1 = match_cats(tol * 0.11, ra1, dec1, ra2, dec2)
    else:
        in1 = match_lists(tol, in_x, in_y, out_x, out_y)
    in2 = in1 != -1
    in3 = in1[in2]
    in4 = np.arange(len(out_x))
    in5 = np.setdiff1d(in4, in3)
    typ_out = np.empty(len(out_x), dtype='<U10')
    typ_out[in3] = typ_in[in2]
    typ_out[in5] = 'other'
    return in1, typ_out


def print_report(filt, test_labels, pred_labels, feat_nms, feat_imp=None, short_rep=True):
    """
    Evaluate the classification model
    - Score the classifier for all classes and each class separately
    - Manually calculate Precision, Recall and Specficity
    - Display the values along with feature importances
    """
    # if feat_imp is None:
    #     feat_imp = []
    score1 = accuracy_score(test_labels, pred_labels)
    score2 = accuracy_score(test_labels[test_labels == 0], pred_labels[test_labels == 0])
    score3 = accuracy_score(test_labels[test_labels == 1], pred_labels[test_labels == 1])
    tp = int(np.ceil(score3 * len(test_labels[test_labels == 1])))
    fn = int(np.ceil((1 - score3) * len(test_labels[test_labels == 1])))
    tn = int(np.ceil(score2 * len(test_labels[test_labels == 0])))
    fp = int(np.ceil((1 - score2) * len(test_labels[test_labels == 0])))
    print('\nBand {:s} feature importance:'.format(filt))
    if not short_rep:
        print('\n Non-point: {:d}'.format(len(test_labels[test_labels == 0])))
        print(' Point:\t\t{:d}\n'.format(len(test_labels[test_labels == 1])))
        print(' Tp:\t\t{:d}\n Fp:\t\t{:d}\n Tn:\t\t{:d}\n Fn:\t\t{:d}\n'.format(tp, fp, tn, fn))
        print(' All:\t\t{:.2f}\n Non-point:\t{:.2f}\n Point:\t\t{:.2f}\n'.format(score1, score2, score3))
        print(' Precision:\t{:.2f}'.format(tp / (tp + fp)))
    # _tmp = [print('{:s}:\t{:.3f}'.format(feat_nms[i],feat_imp[i]))
    #        for i in range(len(feat_nms))]
    # print('\n Precision:\t{:.2f}'.format(tp/(tp+fp)))
    print(' Recall:\t{:.2f} (Sensitivity)'.format(tp / (tp + fn)))
    print(' Specificity:\t{:.2f}\n'.format(tn / (tn + fp)))
    return print('\n')


def make_plots(in_df, out_df, new_labels,
               sky_coord=SKY_COORD, fileroot='', nameroot='',
               filters=FILTERS,
               tol=5., ref_fits=0.,
               use_radec=False,
               show_plot=False):
    """
    Produce figures and text to qualitatively evaluate practicality
    of the classification model for the intended use case of maximizing
    star identification in realistic catalogs
    """
    print("IN MAKE PLOTS")

    def paired_in(a, b, c):
        return input_pair(in_df, a, b, c)

    def paired_out(a, b):
        return output_pair(out_df, new_labels, a, b)

    for i in range(len(filters) - 1):
        for j in range(i, len(filters) - 1):
            radec1 = {'opt': use_radec,
                      'wcs1': sky_coord[i], 'wcs2': sky_coord[j + 1]}
            radec2 = {'opt': use_radec,
                      'wcs1': sky_coord[i], 'wcs2': sky_coord[ref_fits]}
            in_pair, out_pair = paired_in(i, j, radec1), paired_out(i, j)
            cln_pair = clean_pair(in_pair, out_pair, tol=tol, radec=radec2)
            make_cmd_and_xy(in_pair, out_pair, cln_pair,
                            fileroot=fileroot, tol=tol, filepre=nameroot,
                            filt1=filters[i], filt2=filters[j + 1],
                            ab_vega1=AB_VEGA[i], ab_vega2=AB_VEGA[j + 1],
                            opt=['input', 'output', 'clean', 'diff'],
                            radec=radec2, show_plot=show_plot)
    return print('\n')


def make_cmd_and_xy(all_in={}, all_out={}, clean_out={},
                    filt1='', filt2='', ab_vega1=0., ab_vega2=0.,
                    fileroot='', tol=5., filepre='',
                    opt=None, radec=None, show_plot=False):
    """
    Produce color-magnitude diagrams and systematic offsets
    """
    if radec is None:
        radec = {'opt': False, 'wcs1': '', 'wcs2': ''}
    if opt is None:
        opt = ['input', 'output', 'clean', 'diff']
    print('\nFilters {:s} and {:s}:'.format(filt1, filt2))
    print('\n pre: {:s}'.format(filepre))

    def plot_me(a, b, st, ot, ttl, pre, post):
        return plot_cmd(a, b, filt1=filt1, filt2=filt2, stars=st, other=ot, title=ttl,
                        fileroot=fileroot, outfile='_'.join((filepre, 'cmd', filt1, filt2, post)), show_plot=show_plot)

    def plot_it(a, b, filt):
        return plot_xy(x=a, y=a - b, ylim1=-1.0, ylim2=1.0, xlim1=18.5, xlim2=28,
                       ylabel='magIn - magOut', xlabel='magOut', title='In-Out Mag Diff {:s}'.format(filt),
                       fileroot=fileroot, outfile='_'.join((filepre, 'mag', 'diff', filt)), show_plot=show_plot)

    m1_in, m2_in, typ_in = np.array([])
    if ('input' in opt) & (len(all_in) > 0):
        m1_in, m2_in, typ_in = all_in['m1_in'], all_in['m2_in'], all_in['typ_in']
        stars, other = typ_in == 'point', typ_in != 'point'
        print('Stars: {:d}  Others: {:d}'.format(int(np.sum(stars)), int(np.sum(other))))
        plot_me(m1_in, m2_in, stars, other,
                'Input CMD (Vega)', 'input', 'Vega')
    if ('output' in opt) & (len(all_out) > 0):
        m1, m2 = all_out['mag'][0], all_out['mag'][1]
        if 'input' in opt:
            in_x, in_y, out_x, out_y = all_in['X'], all_in['Y'], all_out['xy'][0], all_out['xy'][1]
            in1, typ_out = match_in_out(tol, in_x, in_y, out_x, out_y, typ_in, radec=radec)
            # stars, other = typ_out == 'point', typ_out != 'point'
            if ('diff' in opt) | ('diff2' in opt):
                t1 = (in1 != -1) & (typ_in == 'point')
                m1in, m2in, m1t, m2t = m1_in[t1], m2_in[t1], m1[in1[t1]], m2[in1[t1]]
                t2 = typ_out[in1[t1]] == 'point'
                m1in, m2in, m1t, m2t = m1in[t2], m2in[t2], m1t[t2], m2t[t2]
                if 'diff' in opt:
                    plot_it(m1in, m1t, filt1)
                if 'diff2' in opt:
                    plot_it(m2in, m2t, filt2)
        else:
            typ_out = np.repeat('other', len(m1))
        stars, other = typ_out == 'point', typ_out != 'point'
        print('Stars: {:d}  Others: {:d}'.format(int(np.sum(stars)), int(np.sum(other))))
        plot_me(m1, m2, stars, other, 'Full CMD', 'output', 'full')
    if ('clean' in opt) & (len(clean_out) > 0):
        m1, m2, typ_out = clean_out['m1'], clean_out['m2'], clean_out['typ_out']
        stars, other = typ_out == 'point', typ_out != 'point'
        print('Stars: {:d}  Others: {:d}'.format(int(np.sum(stars)), int(np.sum(other))))
        plot_me(m1, m2, stars, other, 'Cleaned CMD', 'clean', 'clean')
        rr, fr = get_stat(all_in['typ_in'], clean_out['typ_out'])
        print('Recovery Rate:\t {:.2f}\nFalse Rate: \t {:.2f}\n'.format(rr, fr))
    return print('\n')


def plot_cmd(m1, m2, e1=[], e2=[], filt1='', filt2='', stars=[], other=[],
             fileroot='', outfile='test', fmt='png',
             xlim1=-1.5, xlim2=3.5, ylim1=28.5, ylim2=16.5, n=4,
             title='', show_plot=False):
    """
    Produce color-magnitude diagrams
    """
    print("IN PLOT CMD")
    m1m2 = m1 - m2
    plt.rc("font", family='serif', weight='bold')
    plt.rc("xtick", labelsize=15)
    plt.rc("ytick", labelsize=15)
    fig = plt.figure(1, (10, 10))
    fig.suptitle(title, fontsize=5 * n)
    if np.sum(stars) == 0:
        m1m2t, m2t = plot_hess(m1m2, m2)
        plt.plot(m1m2t, m2t, 'k.', markersize=2, alpha=0.75, zorder=3)
    else:
        plt.plot(m1m2[stars], m2[stars], 'b.', markersize=2,
                 alpha=0.75, zorder=2, label='Stars: %d' % len(m2[stars]))
        plt.plot(m1m2[other], m2[other], 'k.', markersize=1,
                 alpha=0.5, zorder=1, label='Other: %d' % len(m2[other]))
        plt.legend(loc=4, fontsize=20)
    # if len(e1) & len(e2):
    #     m1m2err = np.sqrt(e1 ** 2 + e2 ** 2)
    #     plot_error_bars(m2, e2, m1m2err, xlim1, xlim2, ylim1, slope=[])
    plt.xlim(xlim1, xlim2)
    plt.ylim(ylim1, ylim2)
    plt.xlabel(str(filt1 + '-' + filt2), fontsize=20)
    plt.ylabel(filt2, fontsize=20)
    print('\t\t\t Writing out: ', fileroot + outfile + '.' + str(fmt))
    plt.savefig(fileroot + outfile + '.' + str(fmt))
    if show_plot:
        plt.show()
    return plt.close()


def plot_xy(x, y, xlabel='', ylabel='', title='', stars=[], other=[],
            xlim1=-1., xlim2=1., ylim1=-7.5, ylim2=7.5,
            fileroot='', outfile='test', fmt='png', n=4,
            show_plot=False):
    """
    Custom scatterplot maker
    """
    plt.rc("font", family='serif', weight='bold')
    plt.rc("xtick", labelsize=15)
    plt.rc("ytick", labelsize=15)
    fig = plt.figure(1, (10, 10))
    fig.suptitle(title, fontsize=5 * n)
    if not len(x[other]):
        plt.plot(x, y, 'k.', markersize=1, alpha=0.5)
    else:
        plt.plot(x[stars], y[stars], 'b.', markersize=2,
                 alpha=0.5, zorder=2, label='Stars: %d' % len(x[stars]))
        plt.plot(x[other], y[other], 'k.', markersize=1,
                 alpha=0.75, zorder=1, label='Other: %d' % len(x[other]))
        plt.legend(loc=4, fontsize=20)
    plt.xlim(xlim1, xlim2)
    plt.ylim(ylim1, ylim2)
    plt.xlabel(xlabel, fontsize=20)
    plt.ylabel(ylabel, fontsize=20)
    plt.savefig(fileroot + outfile + '.' + str(fmt))
    # print('\t\t\t Writing out: ',fileroot+outfile+'.'+str(fmt))
    if show_plot:
        plt.show()
    return plt.close()


def plot_hess(color, mag, binsize=0.1, threshold=25):
    """
    Overplot hess diagram for densest regions
    of a scatterplot
    """
    if not len(color) > threshold:
        return color, mag
    # mmin, mmax = np.amin(mag), np.amax(mag)
    cmin, cmax = np.amin(color), np.amax(color)
    nmbins = np.ceil((cmax - cmin) / binsize)
    ncbins = np.ceil((cmax - cmin) / binsize)
    hist_value, x_ticks, y_ticks = np.histogram2d(color, mag, bins=(ncbins, nmbins))
    x_ctrds = 0.5 * (x_ticks[:-1] + x_ticks[1:])
    y_ctrds = 0.5 * (y_ticks[:-1] + y_ticks[1:])
    y_grid, x_grid = np.meshgrid(y_ctrds, x_ctrds)
    masked_hist = np.ma.array(hist_value, mask=(hist_value == 0))
    levels = np.logspace(np.log10(threshold),
                         np.log10(np.amax(masked_hist)), (nmbins / ncbins) * 20)
    if (np.amax(masked_hist) > threshold) & (len(levels) > 1):
        cntr = plt.contourf(x_grid, y_grid, masked_hist, cmap=cm.jet, levels=levels, zorder=0)
        cntr.cmap.set_under(alpha=0)
        x_grid, y_grid, masked_hist = x_grid.flatten(), y_grid.flatten(), hist_value.flatten()
        x_grid = x_grid[masked_hist > 2.5 * threshold]
        y_grid = y_grid[masked_hist > 2.5 * threshold]
        mask = np.zeros_like(mag)
        for col, m in zip(x_grid, y_grid):
            mask[(m - binsize < mag) & (m + binsize > mag) &
                 (col - binsize < color) & (col + binsize > color)] = 1
            mag = np.ma.array(mag, mask=mask)
            color = np.ma.array(color, mask=mask)
    return color, mag


def xy_to_wcs(xy, _w):
    """
    Convert pixel coordinates (xy) to astronomical
    coordinated (RA and DEC)
    """
    _radec = _w.wcs_pix2world(xy, 1)
    return _radec[:, 0], _radec[:, 1]


def get_stat(typ_in, typ_out):
    """
    Return recovery rate and false rate for stars
    """
    all_in, all_recov = len(typ_in), len(typ_out)
    stars_in = len(typ_in[typ_in == 'point'])
    stars_recov = len(typ_out[typ_out == 'point'])
    recovery_rate = (stars_recov / stars_in)
    false_rate = 1 - (stars_recov / all_recov)
    return recovery_rate, false_rate


def parse_all():
    parser = wp.PARSER
    parser.add_argument('--C', '-c', type=int, dest='config_id',
                        help='Configuration ID')
    parser.add_argument('--T', '-t', type=int, dest='target_id',
                        help='Target ID')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_all()
    if args.config_id:
        myConfig = wp.Configuration(args.config_id)
        # cull_photometry(myConfig)
    elif args.target_id:
        myTarget = wp.Target(int(args.target_id))
        pid = myTarget.pipeline_id
        allConf = myTarget.configurations
        for myConfig in allConf:
            print(myConfig)
            # cull_photometry(myConfig)
    else:
        this_job = wp.ThisJob
        this_event = wp.ThisEvent
        dp_id = this_event.options['dp_id']
        print(this_job.config_id)
        myConfig = this_job.config
        cull_photometry(myConfig, dp_id)
