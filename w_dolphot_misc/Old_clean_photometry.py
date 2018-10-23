#! /usr/bin/env python
'''
The clean_photometry.py script uses recursive outlier rejection 
to identify stars from noisy astronomical catalogs containing stars,
galaxies and noise from many different sources. 

With all dependencies installed (python3, Pandas, NumPy, 
SciPy, AstroPy, Matplotlib etc.) the simplest use case is:

./clean_photomtry.py $path/filename.phot

where filename.phot is the raw DOLPHOT photomtery output. 

This tool is built as part of the WFIRST simulations, analysis and 
recommendation pipeline for carrying out Nearby Galaxies projects. 
The current implementation requires STIPS simulation input catalogs
and  optionally uses STIPS simulated images.

- Rubab Khan
rubab@uw.edu

'''
import time, argparse, concurrent.futures, matplotlib
matplotlib.use('Agg')
from matplotlib import cm
from matplotlib import pyplot as plt
plt.ioff()
import numpy as np
import pandas as pd
from astropy.io import ascii, fits
from astropy import units as u
from astropy import wcs
from astropy.coordinates import SkyCoord, match_coordinates_sky
from scipy.spatial import cKDTree
from os import cpu_count

'''Create worker pools'''
cpu_pool = concurrent.futures.ProcessPoolExecutor(max_workers=cpu_count())


'''
Therese parameters are used throughout the code:

feature_names: DOLPHOT quality parameters to use for 
training Machine Learning models. 

filters: WFIRST filters used in the simulation.

AB_Vega: Offsets between AB and Vega magnitude systems

fits_files, ref_fits, use_radec: Simulated images may 
be misaligned by design to emulate real observational
conditions.

'''

# filter names
filters    = np.array(['Z087','Y106','J129','H158','F184'])

# AB magnitude Zero points
AB_Vega    = np.array([0.487,  0.653, 0.958, 1.287, 1.552])

# Simulated images
fits_files = ["sim_1_0.fits","sim_2_0.fits","sim_3_0.fits",
              "sim_4_0.fits","sim_5_0.fits"]
sky_coord  = np.zeros(len(filters))
ref_fits   = int(3)
use_radec  = False


def cleanAll(filename,filters=filters,AB_Vega=AB_Vega,
             fits_files=fits_files,ref_fits=ref_fits,
             sky_coord=sky_coord,use_radec=use_radec,
             tol=1,niter=10,nsig=10,sigres=0.05,
             sigcut=3.2,FR=0.4,RR=0.9,valid_mag=30):
    fileroot,filename = get_fileroot(filename)

    if use_radec:
        sky_coord = [wcs.WCS(fits.open(fileroot+imfile)[1].header) \
              for imfile in fits_files]
        
    sigcuts = np.arange(sigcut-sigres*nsig/2,sigcut+sigres*nsig/2,sigres)
    
    input_data,output_data  = read_data(filename=filename,
                                        fileroot=fileroot,
                                        filters=filters)
    
    in_DF,out_DF = prep_data(input_data,output_data,
                             filters=filters,
                             valid_mag=valid_mag)
    
    _in,_out,_i,_j,_tol,_sigs,_nit,_file,_root,_FR,_RR,_best,_plots,_radec=\
                            [],[],[],[],[],[],[],[],[],[],[],[],[],[]
    
    paired_in    = lambda a,b,c: input_pair(in_DF,a,b,c)
    paired_out   = lambda a,b: output_pair(out_DF,a,b)

    for i in range(len(filters)-1):
        for j in range(i,len(filters)-1):
            radec1 = {'opt':use_radec,
                      'wcs1':sky_coord[i],'wcs2':sky_coord[j+1]}
            radec2 = {'opt':use_radec,
                      'wcs1':sky_coord[i],'wcs2':sky_coord[ref_fits]}
            inPair,outPair = paired_in(i,j,radec1),paired_out(i,j)   
    
            _t1,_t2 = paired_in(i,j,radec1),paired_out(i,j)        
            _in.append(_t1); _out.append(_t2); _i.append(i); _j.append(j)
            _tol.append(tol);_sigs.append(sigcuts);_nit.append(niter)
            _file.append(filename);_root.append(fileroot)
            _radec.append(radec2)
            _FR.append(FR);_RR.append(RR);_best.append(True);_plots.append(True)
    
    _t = cpu_pool.map(clean_all,_in,_out,_i,_j,\
                      _tol,_sigs,_nit,_RR,_FR,_file,_root,_best,_plots,_radec,\
                      chunksize=int(np.ceil(len(_i)/cpu_count())))
    '''
    _t = clean_all(_in[0],_out[0],_i[0],_j[0],\
                    _tol[0],_sigs[0],_nit[0],
                    _RR[0],_FR[0],_file[0],_root[0],
                    _best[0],_plots[0],_radec[0])
    '''
    return 1


def read_data(filename='10_10_phot.txt',fileroot='',filters=filters):
    '''
    Read in the raw fata files:
    - Input: sythetic photometry file for image generation, IPAC format
    - Output: DOLPHOT measured raw photometry, ASCII format
    
    Return arrays of AstroPy tables for input and numpy arrays for output
    ordered by corresponding filternames.
    '''
    input_data = [ascii.read(fileroot+filt+'_stips.txt',format='ipac')
                  for filt in filters]
    output_data  = np.loadtxt(fileroot+filename)
    return input_data,output_data

def prep_data(input_data,output_data,
              filters=filters,
              valid_mag=30):
    '''
    Prepare the data for classification. The output data is now cleaned 
    to exclude low information entries

    Return 2 arrays ordered by corresponding filternames:
    - First array for input data in pandas data frames
    - Second array for cleaned output data in pandas data frames
    '''
    nfilt = filters.size
    xy         = output_data[:,2:4].T
    Count      = output_data[:,range(13,13+13*nfilt,13)].T
    vega_mags  = output_data[:,range(15,15+13*nfilt,13)].T
    mag_errors = output_data[:,range(17,17+13*nfilt,13)].T
    SNR        = output_data[:,range(19,19+13*nfilt,13)].T
    Sharp      = output_data[:,range(20,20+13*nfilt,13)].T
    Round      = output_data[:,range(21,21+13*nfilt,13)].T
    Crowd      = output_data[:,range(22,22+13*nfilt,13)].T    
    
    in_df,out_df = [],[]
    
    for i in range(nfilt):
        in_df.append(pack_input(input_data[i],valid_mag=valid_mag))
        
        t = validate_output(mag_errors[i],
                            Count[i],SNR[i],
                            Sharp[i],Round[i],
                            Crowd[i])
        
        out_df.append(pack_output(xy,vega_mags[i],mag_errors[i],
                                  Count[i],SNR[i],Sharp[i],Round[i],
                                  Crowd[i],t))
        
    return in_df,out_df


def validate_output(err,count,snr,shr,rnd,crd):
    '''
    Clean and validate output data
    - Remove measurements with unphysical values, such as negative countrate
    - Remove low information entries, such as magnitude errors >0.5 & SNR <1
    - Remove missing value indicators such as +/- 9.99
    '''
    return (err<0.5)&(count>=0)&(snr>=1)&(crd!=9.999)&\
        (shr!=9.999)&(shr!=-9.999)&(rnd!=9.999)&(rnd!=-9.999)


def pack_input(data,valid_mag=30):
    '''
    return Pandas Dataframes for input AstroPy tables containing 
    sources that are brighter than specified magnitude (valid_mag)
    '''
    t = data['vegamag'] < valid_mag
    return pd.DataFrame({'x':data['x'][t],'y':data['y'][t],\
                         'm':data['vegamag'][t],'type':data['type'][t]})


def pack_output(xy,mags,errs,count,snr,shr,rnd,crd,t):
    '''
    return Pandas Dataframes for output numpy arrays including
    all quality parameter
    '''
    return pd.DataFrame({'x':xy[0][t],'y':xy[1][t],'mag':mags[t],'err':errs[t],
                        'Count':count[t],'SNR':snr[t],'Sharpness':shr[t],
                         'Roundness':rnd[t],'Crowding':crd[t]})


def input_pair(df,i,j,radec={'opt':False,'wcs1':'','wcs2':''}):
    '''
    Pick sources added in both bands as same object types
    
    return data dictionary containing the two input magnitudes 
    (m1_in, m2_in), coordinates (X, Y) and input source type
    (typ_in)
    '''
    m1_in,m2_in,X1,Y1,X2,Y2 = df[i]['m'].values,df[j+1]['m'].values,\
        df[i]['x'].values,df[i]['y'].values,\
        df[j+1]['x'].values,df[j+1]['y'].values
    typ1_in, typ2_in = df[i]['type'].values, df[j+1]['type'].values

    if radec['opt']:
        ra1,dec1 = xy_to_wcs(np.array([X1,Y1]).T,radec['wcs1'])
        ra2,dec2 = xy_to_wcs(np.array([X2,Y2]).T,radec['wcs2'])
        in12= matchCats(0.05,ra1,dec1,ra2,dec2)
    else:
        in12 = matchLists(0.1,X1,Y1,X2,Y2)

    m1_in,X1,Y1,typ1_in = m1_in[in12!=-1],\
        X1[in12!=-1],Y1[in12!=-1],typ1_in[in12!=-1]
    in12 = in12[in12!=-1]
    m2_in,typ2_in = m2_in[in12],typ2_in[in12]
    
    tt = typ1_in==typ2_in
    m1_in,m2_in,X,Y,typ_in = m1_in[tt],\
        m2_in[tt],X1[tt],Y1[tt],typ1_in[tt]
    return dict(zip(['m1_in','m2_in','X','Y','typ_in'],[m1_in,m2_in,X,Y,typ_in]))


'''Recovered source photometry and quality params'''
def output_pair(df,i,j):
    '''
    Pick sources detected in both bands as same object types
    
    return data dictionary containing the two output magnitudes (mag) 
    coordinates (xy), all quality parameters (err,snr,crd,rnd,shr)
    and labels (lbl). Each dictionary item is has two elements for 
    two filters (xy has x and y).
    '''
    X1,Y1,X2,Y2 = df[i]['x'].values,df[i]['y'].values,\
                  df[j+1]['x'].values,df[j+1]['y'].values
    t2 = matchLists(0.1,X1,Y1,X2,Y2)
    t1 = t2!=-1
    t2 = t2[t2!=-1] 
    xy = X1[t1],Y1[t1]
    mags = [df[i]['mag'][t1].values,df[j+1]['mag'][t2].values]
    errs = [df[i]['err'][t1].values,df[j+1]['err'][t2].values]
    snrs = [df[i]['SNR'][t1].values,df[j+1]['SNR'][t2].values]
    crds = [df[i]['Crowding'][t1].values,df[j+1]['Crowding'][t2].values]
    rnds = [df[i]['Roundness'][t1].values,df[j+1]['Roundness'][t2].values]
    shrs = [df[i]['Sharpness'][t1].values,df[j+1]['Sharpness'][t2].values]
    nms = ['xy','mag','err','snr','crd','rnd','shr']
    K = [xy,mags,errs,snrs,crds,rnds,shrs]
    return dict(zip(nms,K))



def clean_all(all_in,all_out,i=0,j=0,tol=1,sigcuts=[3.0,3.2,3.5],niter=10,
              RR=0.9,FR=0.25,filename='',fileroot='',best=True,plots=False,
              radec={'opt':False,'wcs1':'','wcs2':''},
              show_plot=False):
    X,Y,typ_in,x,y,m1,m2,K = unpack_inOut(all_in,all_out)

    ''' Order of quality param use '''
    nms = ['rnd','shr','err','crd','snr']
    #nms = ['err','shr','rnd']
    myClean = lambda a: clean_up(X,Y,typ_in,x,y,m1,m2,K,nms,\
                    tol=tol,sigcut=a,niter=niter,RR=RR,FR=FR,
                    radec=radec)
    _rr,_fr,_tt = [],[],[]
    for sigcut in sigcuts:
        _r,_f,_t = myClean(sigcut)
        _rr.append(_r); _fr.append(_f); _tt.append(_t)
    _rr,_fr,_tt,_sig = np.array(_rr), np.array(_fr), np.array(_tt), np.array(sigcuts)
   
    if len(_fr[_fr<FR])>0:
        t = _fr<FR
        _rr,_fr,_tt,_sig = _rr[t],_fr[t],_tt[t],_sig[t]
        if len(_rr)>1:
            t = np.argmax(_rr)
            _rr,_fr,_tt, _sig = _rr[t],_fr[t],_tt[t],_sig[t]   
    else:
        t = np.argmin(_fr)
        _rr,_fr,_tt, _sig = _rr[t],_fr[t],_tt[t],_sig[t]
    
    t = _tt==True
    if best:
        show_cuts(K,nms,t,_sig,filters[i],filters[j+1])
        
    if plots:
        tmp, typ_out = match_in_out(tol,X,Y,x[t],y[t],typ_in,radec=radec)
        clean_out = dict(zip(['m1','m2','x','y','typ_out'],[m1[t],m2[t],x[t],y[t],typ_out]))
        
        make_plots(all_in,all_out,clean_out,\
                   filt1=filters[i],filt2=filters[j+1],\
                   AB_Vega1=AB_Vega[i],AB_Vega2=AB_Vega[j+1],\
                   fileroot=fileroot,tol=tol,\
                   opt=['input','output','clean','diff'],\
                   radec=radec,show_plot=show_plot)            
    return 1
    

def show_cuts(K,nms,t,wd=3.2,filt1='',filt2='',wt=2):
    print('\nFilters: {:s} and {:s}\nCuts:'.format(filt1,filt2))
    for s in range(len(nms)):
        xt,yt = K[nms[s]][0][t], K[nms[s]][1][t]
        xtm,xts,ytm,yts = xt.mean(), xt.std(), yt.mean(), yt.std()
        xlo,xhi,ylo,yhi = xtm-wd*xts, xtm+wd*xts, ytm-wd*yts, ytm+wd*yts
        if (nms[s]=='err')|(nms[s]=='crd'):
            xlo,ylo = 0,0
        elif nms[s]=='snr':
            xhi,yhi = xhi*wt,yhi*wt
            if xlo<1: xlo=1
            if ylo<1: ylo=1
        print('{:s}\t{:.2f}\t{:.2f}\t{:.2f}\t{:.2f}'.format(nms[s],xlo,xhi,ylo,yhi))
    return 1


'''Iteratively reject outliers until desired false-rate is achieved'''
def clean_up(X,Y,typ_in,x,y,m1,m2,K,nms,tol=1,sigcut=3.5,niter=10,RR=0.9,FR=0.25,
              radec={'opt':False,'wcs1':'','wcs2':''}):
    rr,fr,t = 0, 1, np.repeat(True,len(x))
    for z in range(niter):
        if (fr>FR):
            for s in range(len(nms)):
                tmp, typ_out = match_in_out(tol,X,Y,x[t],y[t],typ_in,radec=radec)
                k0,k1 = K[nms[s]][0][t], K[nms[s]][1][t]
                p0,p1 = k0[typ_out=='point'], k1[typ_out=='point']
                t1 = myCut(k0,k1,p0,p1,sigcut,nms[int(s)])
                t[t] = t1
            tmp, typ_out = match_in_out(tol,X,Y,x[t],y[t],typ_in,radec=radec)
            rr,fr = get_stat(typ_in,typ_out)
        else:
            break
    # Also return the cuts
    return rr,fr,t


'''Sigma outlier culling'''
def myCut(x,y,xt,yt,wd=3.5,nms='',wt=2):
    xtm,xts,ytm,yts = xt.mean(), xt.std(), yt.mean(), yt.std()
    xlo,xhi,ylo,yhi = xtm-wd*xts, xtm+wd*xts, ytm-wd*yts, ytm+wd*yts
    if (nms=='err')|(nms=='crd'):
        return (x<xhi) & (y<yhi)
    elif (nms=='rnd')|(nms=='shr'):
        return (x<xhi)&(x>xlo)&(y<yhi)&(y>ylo)
    elif nms=='snr':
        if xlo<1: xlo=1
        if ylo<1: ylo=1
        return (x<xhi*wt)&(x>xlo)&(y<yhi*wt)&(y>ylo)

    
def matchLists(tol,x1,y1,x2,y2):
    '''
    Match X and Y coordinates using cKDTree
    return index of 2nd list at coresponding position in the 1st 
    return -1 if no match is found within matching radius (tol)
    '''
    d1 = np.empty((x1.size, 2))
    d2 = np.empty((x2.size, 2))
    d1[:,0],d1[:,1] = x1,y1
    d2[:,0],d2[:,1] = x2,y2
    t = cKDTree(d2)
    tmp, in1 = t.query(d1, distance_upper_bound=tol)
    in1[in1==x2.size] = -1
    return in1


def matchCats(tol,ra1,dec1,ra2,dec2):
    '''
    Match astronomical coordinates using SkyCoord
    return index of 2nd list at coresponding position in the 1st 
    return -1 if no match is found within matching radius (tol)
    '''
    c1 = SkyCoord(ra=ra1*u.degree, dec=dec1*u.degree)
    c2 = SkyCoord(ra=ra2*u.degree, dec=dec2*u.degree)
    in1,sep,tmp = match_coordinates_sky(c1,c2,storekdtree=False)
    sep = sep.to(u.arcsec)
    in1[in1==ra2.size] = -1
    in1[sep>tol*u.arcsec] = -1
    return in1


def match_in_out(tol,X,Y,x,y,typ_in,
                 radec={'opt':False,'wcs1':'','wcs2':''}):
    '''
    Match input coordnates to recovered coordinates picking the 
    closest matched item.
    
    return index of output entry at coresponding position in the 
    input list and source type of the matching input
    
    return -1 as the index if no match is found and source type 
    as 'other' (not point source)
    '''
    if radec['opt']:
        ra1,dec1 = xy_to_wcs(np.array([X,Y]).T,radec['wcs1'])
        ra2,dec2 = xy_to_wcs(np.array([x,y]).T,radec['wcs2'])
        in1 = matchCats(tol*0.11,ra1,dec1,ra2,dec2)
    else:
        in1 = matchLists(tol,X,Y,x,y)

    in2 = in1!=-1
    in3 = in1[in2]
    in4 = np.arange(len(x))
    in5 = np.setdiff1d(in4,in3)
    typ_out = np.empty(len(x),dtype='<U10')
    typ_out[in3] = typ_in[in2]
    typ_out[in5] = 'other'
    return in1, typ_out


def get_stat(typ_in,typ_out):
    ''' Return recovery rate and false rate for stars'''
    all_in, all_recov = len(typ_in), len(typ_out)
    stars_in = len(typ_in[typ_in=='point'])
    stars_recov = len(typ_out[typ_out=='point'])
    recovery_rate = (stars_recov / stars_in)
    false_rate = 1 - (stars_recov / all_recov)
    return recovery_rate,false_rate


'''Take quality cuts on recovered'''
def unpack_inOut(all_in,all_out,tol=1,n=5):
    X,Y,typ_in = all_in['X'], all_in['Y'], all_in['typ_in']
    x,y,m1,m2 = all_out['xy'][0], all_out['xy'][1],\
                all_out['mag'][0], all_out['mag'][1]
    err,rnd,shr,crd,snr = all_out['err'],all_out['rnd'],\
            all_out['shr'],all_out['crd'],all_out['snr']
    K = [[rnd[0],rnd[1]],[shr[0],shr[1]],[err[0],err[1]],\
          [crd[0],crd[1]],[snr[0],snr[1]]]
    nms = ['rnd','shr','err','crd','snr']
    return X,Y,typ_in,x,y,m1,m2,dict(zip(nms,K))


def make_plots(all_in=[],all_out=[],clean_out=[],\
               filt1='',filt2='',AB_Vega1=0,AB_Vega2=0,
               fileroot='',tol=5,
               opt=['input','output','clean','diff'],
               radec={'opt':False,'wcs1':'','wcs2':''},
               show_plot=False):
    '''Produce color-magnitude diagrams and systematic offsets'''
    print('\nFilters {:s} and {:s}:'.format(filt1,filt2))
    plot_me = lambda a,b,st,ot,ttl,pre,post: \
              plot_cmd(a,b,filt1=filt1,filt2=filt2,\
                       stars=st,other=ot,title=ttl,\
                       fileroot=fileroot,outfile=\
                       '_'.join((pre,'cmd',filt1,filt2,post)),\
                       show_plot=show_plot)
    plot_it = lambda a,b,filt: \
              plot_xy(x=a,y=a-b,\
                      ylim1=-1.5,ylim2=0.5,xlim1=24.5,xlim2=28,\
                      ylabel='magIn - magOut',xlabel='magOut',\
                      title='In-Out Mag Diff {:s}'.format(filt),\
                      fileroot=fileroot,\
                      outfile='_'.join(('mag','diff',filt)),\
                      show_plot=show_plot)

    if (('input' in opt)&(len(all_in)>0)):
        m1_in,m2_in,typ_in = all_in['m1_in'],all_in['m2_in'],all_in['typ_in']
        stars,other = typ_in=='point',typ_in!='point'
        print('Stars: {:d}  Others: {:d}'.format(int(np.sum(stars)),int(np.sum(other))))
        plot_me(m1_in,m2_in,stars,other,\
                'Input CMD (Vega)','input','Vega')
        #plot_me(m1_in+AB_Vega1,m2_in+AB_Vega2,stars,other,\
        #        'Input CMD (AB)','input','AB')

    if (('output' in opt)&(len(all_out)>0)):
        m1,m2 = all_out['mag'][0], all_out['mag'][1]
        if 'input' in opt:
            X,Y,x,y = all_in['X'],all_in['Y'], all_out['xy'][0], all_out['xy'][1]
            in1, typ_out = match_in_out(tol,X,Y,x,y,typ_in,radec=radec)
            stars,other = typ_out=='point',typ_out!='point'
            if (('diff' in opt)|('diff2' in opt)):
                t1 = (in1!=-1)&(typ_in=='point')
                m1in,m2in,m1t,m2t = m1_in[t1],m2_in[t1],m1[in1[t1]],m2[in1[t1]]
                t2 = typ_out[in1[t1]]=='point'
                m1in,m2in,m1t,m2t=m1in[t2],m2in[t2],m1t[t2],m2t[t2]
                if 'diff' in opt:
                    plot_it(m1in,m1t,filt1)
                if 'diff2' in opt:
                    plot_it(m2in,m2t,filt2)
        else:
            typ_out = np.repeat('other',len(m1))
        stars,other = typ_out=='point',typ_out!='point'
        print('Stars: {:d}  Others: {:d}'.format(int(np.sum(stars)),int(np.sum(other))))
        plot_me(m1,m2,stars,other,'Full CMD','output','full')

    if (('clean' in opt)&(len(clean_out)>0)):
        m1,m2,typ_out = clean_out['m1'],clean_out['m2'],clean_out['typ_out']
        stars,other = typ_out=='point',typ_out!='point'
        print('Stars: {:d}  Others: {:d}'.format(int(np.sum(stars)),int(np.sum(other))))
        plot_me(m1,m2,stars,other,'Cleaned CMD','clean','clean')
        rr,fr = get_stat(all_in['typ_in'],clean_out['typ_out'])
        print('Recovery Rate:\t {:.2f}\nFalse Rate: \t {:.2f}\n'.format(rr,fr))
    return print('\n')


def plot_cmd(m1,m2,e1=[],e2=[],filt1='',filt2='',stars=[],other=[],\
             fileroot='',outfile='test',fmt='png',\
             xlim1=-1.5,xlim2=3.5,ylim1=29.5,ylim2=20.5,n=4,
             title='',show_plot=False):
    '''Produce color-magnitude diagrams'''
    m1m2 = m1-m2
    plt.rc("font", family='serif', weight='bold')
    plt.rc("xtick", labelsize=15); plt.rc("ytick", labelsize=15)
    fig = plt.figure(1, ((10,10)))
    fig.suptitle(title,fontsize=5*n)
    if len(stars[stars])==0:
        m1m2t,m2t = plotHess(m1m2,m2)
        plt.plot(m1m2t,m2t,'k.',markersize=2,alpha=0.75,zorder=3)
    else:
        plt.plot(m1m2[stars],m2[stars],'b.',markersize=2,\
            alpha=0.75,zorder=2,label='Stars: %d' % len(m2[stars]))
        plt.plot(m1m2[other],m2[other],'k.',markersize=1,\
            alpha=0.5,zorder=1,label='Other: %d' % len(m2[other]))
        plt.legend(loc=4,fontsize=20)
    if (len(e1)&len(e2)):
        m1m2err = np.sqrt(e1**2+e2**2)
        plot_error_bars(m2,e2,m1m2err,xlim1,xlim2,ylim1,slope=[])
    plt.xlim(xlim1,xlim2); plt.ylim(ylim1,ylim2)
    plt.xlabel(str(filt1+'-'+filt2),fontsize=20)
    plt.ylabel(filt2,fontsize=20)
    print('\t\t\t Writing out: ',fileroot+outfile+'.'+str(fmt))
    plt.savefig(fileroot+outfile+'.'+str(fmt))
    if show_plot: plt.show()
    return plt.close()


def plot_xy(x,y,xlabel='',ylabel='',title='',stars=[],other=[],\
            xlim1=-1,xlim2=1,ylim1=-7.5,ylim2=7.5,\
            fileroot='',outfile='test',fmt='png',n=4,
            show_plot=False):
    '''Custom scatterplot maker'''
    plt.rc("font", family='serif', weight='bold')
    plt.rc("xtick", labelsize=15); plt.rc("ytick", labelsize=15)
    fig = plt.figure(1, ((10,10)))
    fig.suptitle(title,fontsize=5*n)
    if not len(x[other]):
        plt.plot(x, y,'k.',markersize=1,alpha=0.5)
    else:
        plt.plot(x[stars],y[stars],'b.',markersize=2,\
            alpha=0.5,zorder=2,label='Stars: %d' % len(x[stars]))
        plt.plot(x[other],y[other],'k.',markersize=1,\
            alpha=0.75,zorder=1,label='Other: %d' % len(x[other]))
        plt.legend(loc=4,fontsize=20)
    plt.xlim(xlim1,xlim2); plt.ylim(ylim1,ylim2)
    plt.xlabel(xlabel,fontsize=20)
    plt.ylabel(ylabel,fontsize=20)
    plt.savefig(fileroot+outfile+'.'+str(fmt))
    #print('\t\t\t Writing out: ',fileroot+outfile+'.'+str(fmt))
    if show_plot: plt.show()
    return plt.close()


def plotHess(color,mag,binsize=0.1,threshold=25):
    '''Overplot hess diagram for densest regions 
    of a scatterplot'''
    if not len(color)>threshold:
        return color,mag
    mmin,mmax = np.amin(mag),np.amax(mag)
    cmin,cmax = np.amin(color),np.amax(color)
    nmbins = np.ceil((cmax-cmin)/binsize)
    ncbins = np.ceil((cmax-cmin)/binsize)
    Z, xedges, yedges = np.histogram2d(color,mag,\
                            bins=(ncbins,nmbins))
    X = 0.5*(xedges[:-1] + xedges[1:])
    Y = 0.5*(yedges[:-1] + yedges[1:])
    y, x = np.meshgrid(Y, X)
    z = np.ma.array(Z, mask=(Z==0))
    levels = np.logspace(np.log10(threshold),\
            np.log10(np.amax(z)),(nmbins/ncbins)*20)
    if (np.amax(z)>threshold)&(len(levels)>1):
        cntr=plt.contourf(x,y,z,cmap=cm.jet,levels=levels,zorder=0)
        cntr.cmap.set_under(alpha=0)
        x,y,z = x.flatten(),y.flatten(),Z.flatten()
        x = x[z>2.5*threshold]
        y = y[z>2.5*threshold]
        mask = np.zeros_like(mag)
        for col,m in zip(x,y):
            mask[(m-binsize<mag)&(m+binsize>mag)&\
                 (col-binsize<color)&(col+binsize>color)]=1
            mag = np.ma.array(mag,mask=mask)
            color = np.ma.array(color,mask=mask)
    return color,mag


def xy_to_wcs(xy,_w):
    '''Convert pixel coordinates (xy) to astronomical
    coordinated (RA and DEC)'''
    _radec = _w.wcs_pix2world(xy,1)
    return _radec[:,0],_radec[:,1]


def get_fileroot(filename):
    '''return path to a file and filename'''
    if '/' in filename:
        tmp = filename.split('/')[-1]
        fileroot = filename[:-len(tmp)]
        filename = tmp
    else:
        fileroot = ''
    return fileroot, filename



'''Argument parser template'''
def parse_all():
    parser = argparse.ArgumentParser()
    parser.add_argument('filenames', nargs='+',help='Photomtery file names')
    parser.add_argument('--NITER', '-niter', type=int, dest='niter', default=10, help='Maximum iterations')
    parser.add_argument('--NSIG', '-nsig', type=int, dest='nsig', default=4, help='# of other sigma thresholds to try')
    parser.add_argument('--SIGRES', '-sigres', type=float, dest='sigres', default=0.1, help='Sigma stepsize')
    parser.add_argument('--SIGMA', '-sig', type=float, dest='sig', default=3.5, help='Sigma threshold removing outliers')
    parser.add_argument('--RADIUS', '-tol', type=float, dest='tol', default=5, help='Matching radius in pixels')
    parser.add_argument('--FALSE', '-fr', type=float, dest='fr', default=0.2, help='Desired False Rate')
    parser.add_argument('--RECOV', '-rr', type=float, dest='rr', default=0.5, help='Desired Recovery Rate')
    parser.add_argument('--VALIDMAG', '-mag', type=float, dest='mag', default=30, help='Expected depth in mag')
    return parser.parse_args()


'''If executed from command line'''
if __name__ == '__main__':
    tic = time.time()
    assert 3/2 == 1.5, 'Not running Python3 may lead to wrong results'
    args = parse_all()
    _do = lambda x: cleanAll(x, tol=args.tol,\
                          niter=args.niter, nsig=args.nsig,\
                          sigres=args.sigres, sigcut=args.sig,\
                          FR=args.fr,RR=args.rr)
    for filename in args.filenames:
        _do(filename)
    else:
        cpu_pool.shutdown(wait=True)
        print('\n\nCompleted in %.3f seconds \n' % (time.time()-tic))
