#!/usr/bin/env python
'''
Read in filter+telescope+instrument throughput and produce 
FITS and ASCII tables for filtersets
'''
import numpy as np
import scipy.stats as stats
import scipy.interpolate as interp 
import matplotlib.pyplot as plt
from matplotlib import gridspec
from astropy.io import ascii
from astropy.io import fits
from astropy.table import Table
from astropy import constants as const

filenames  = ['WFI-silver2.txt','WFI-mix2.txt', 'WFI-gold2.txt']
tablenames = ['Silver2','Mix2','Gold2']

filternames = np.array(['X606', 'X670','Z087','Y106','J129','H158','F184','W149'])
filt_set    = np.array([[0.48,0.7],  [0.541,0.8],[0.76,0.977],[0.927,1.192],[1.131,1.454],[1.38,1.774],[1.683,2], [0.927,2]])
filt_set2   = np.array([[0.47,0.73], [0.53,0.83],[0.75,1],    [0.91,1.23],  [1.1,1.5],    [1.35,1.85], [1.65,2.1],[0.9,2.05]])

n_samp, frac_edge_decay, wave_range = 5000, 0.05, [0.4,2.1]

f_nu_h = 3.631e-20 / const.h.cgs.value
ATOT = 3.757e4


def DoAll():    
    for filename,tablename in zip(filenames,tablenames):
        make_filters(filename,tablename)        
    write_summary(tablenames)
    return


def make_filters(filename,tablename,filternames=filternames,filt_set=filt_set,
                 n_samp=n_samp,frac_edge_decay=frac_edge_decay,wave_range=wave_range):
    wave = np.linspace(wave_range[0],wave_range[1],n_samp)
    datafile = ascii.read(filename)
    T  = np.array([datafile[filternames[i]].data for i in range(len(filternames))])
    L = np.array([np.linspace(filt_set2[i][0],filt_set2[i][1],T.shape[1]) for i in range(len(filternames))])
    A = np.array([get_filt_curve(wave,L[i,:],T[i,:]) for i in range(len(filternames))])
    write_fits_table(tablename,A,wave,filternames)
    write_ascii_table(tablename,A,wave,filternames)
    plot_filter_set(tablename,A,wave)
    return


def get_filt_curve(wave,L,T):
    return np.interp(wave, L, T, left=0, right=0)

def temp():
    filt_width = L[-1]-L[0]
    d_edge = filt_width * frac_edge_decay
    n_sig = 4.0
    sig = d_edge / (2.0 * n_sig)
    cen = [L[0] - n_sig * sig, L[-1] + n_sig * sig]
    filt[wave < L[0]]  =  T[0]*stats.norm.cdf(wave[wave < L[0]], loc=cen[0], scale=sig)
    filt[wave > L[-1]] = T[-1]*stats.norm.sf(wave[wave > L[-1]], loc=cen[1], scale=sig)
    filt_curve = interp.splrep(wave,filt,k=2,s=0.1)
    return interp.splev(wave,filt_curve,der=0,ext=1)


def write_fits_table(tablename,A,wave,filternames=filternames):
    filename = 'filter_'+str(len(filternames))+'_'+tablename+'.fits'
    cdef = []
    cdef.append(fits.Column(name='wavelength', format='D', array = wave))
    for i, colstr in enumerate(filternames):        
        cdef.append(fits.Column(name=colstr, format='D', array = A[i,:]))
    tb_hdu = fits.BinTableHDU.from_columns(fits.ColDefs(cdef))
    hdu = fits.PrimaryHDU()
    t_hdu_list = fits.HDUList([hdu, tb_hdu])
    try:
        tb_hdu.writeto(filename)
        print('Wrote to %s ' % filename)
    except:
        print('File %s' % filename, 'already exists. Please delete to replace.')
    return


def write_ascii_table(tablename,A,wave,filternames=filternames):
    filename = 'filter_'+str(len(filternames))+'_'+tablename+'.txt'
    tab = [wave,A[0,],A[1,],A[2,],A[3,],A[4,],A[5,],A[6,],A[7,]]
    nms = ('wavelength','X606','X670','Z087','Y106','J129','H158','F184','W149')
    fmt = {'wavelength':'%15.10f','X606':'%8.4f','X670':'%8.4f','Z087':'%8.4f',\
           'Y106':'%8.4f','J129':'%8.4f','H158':'%8.4f','F184':'%8.4f','W149':'%8.4f'}
    t   = Table(tab, names=nms)
    ascii.write(t, filename, format='fixed_width', delimiter='', formats=fmt)
    return


def write_summary(tablenames,filternames=filternames,filt_set=filt_set,ATOT=ATOT,f_nu_h=f_nu_h):
    outfile = 'filter_'+str(len(filternames))+'_Summary2.txt'
    cntr,lo,hi = (filt_set[:,0]+filt_set[:,1])/2, filt_set[:,0], filt_set[:,1]
    A = np.zeros([3,len(filternames),3])
    for j, fileroot in enumerate(tablenames):
        datafile = ascii.read('filter_'+str(len(filternames))+'_'+fileroot+'.txt')
        print('\n %s :' % fileroot)
        L = datafile['wavelength'].data
        T = np.array([datafile[filternames[i]].data for i in range(len(filternames))])
        A[j,:,:] = np.array([get_filt_stat(L,T[i,:],ATOT,f_nu_h) for i in range(len(filternames))])

    tab  = [filternames,lo,hi,cntr,A[0,:,0],A[1,:,0],A[2,:,0],A[0,:,1],A[1,:,1],A[2,:,1],A[0,:,2],A[1,:,2],A[2,:,2]]
    nms  = ('name','lo','hi','cntr','piv1','piv2','piv3','awp1','awp2','awp3','ZP1','ZP2','ZP3')
    fmt  = {'name':'%5s','lo':'%6.3f','hi':'%6.3f','cntr':'%6.3f','piv1':'%10.3f','piv2':'%6.3f','piv3':'%6.3f',\
            'awp1':'%10.4f','awp2':'%6.4f','awp3':'%6.4f','ZP1':'%10.3f','ZP2':'%6.3f','ZP3':'%6.3f'}
    t    = Table(tab, names=nms)
    ascii.write(t, outfile, format='fixed_width', delimiter='', formats=fmt)
    print('\nWrote to %s \n' % outfile)
    return


def get_filt_stat(L,T,ATOT=ATOT,f_nu_h=f_nu_h):
    L,T = L[T>0.001]*1e-4,T[T>0.001]
    pivot  = np.sqrt(np.mean(T*L)/np.mean(T/L))
    Tmean  = np.mean(T)
    ZP     = 2.5*np.log10(f_nu_h*(Tmean*ATOT)*(Tmean/np.amax(T))*(L[-1]-L[0])/pivot)
    AWP    = (Tmean*ATOT*1e-4)*(Tmean/np.amax(T))*(L[-1]-L[0])/pivot
    pivot  = pivot*1e4
    print('Lambda pivot = %.4f' % pivot,'; Aeff*Weff/pivot = %.4f' % AWP,'; AB zeropint = %.3f' % ZP)
    return np.array([pivot,AWP,ZP])


def plot_filter_set(tablename, A, wave, title='', plotfilename='',
                    show_microlensing_filt=True, save_plot = True,):

    title,plotfilename = tablename, 'filter_'+str(A.shape[0])+'_'+tablename+'.png'
    
    fig = plt.figure(figsize=(12,9))
    
    yrange=[0,1]

    # Plot a background grid for prettiness.
    
    greyval = '#F2F2F2'
    dwave = 0.25
    gap_frac = 0.02
    gap = dwave * gap_frac
    grid_start = gap + dwave * np.arange(0.0, 4.0/dwave)
    grid_end = grid_start + (dwave - 2.0*gap)

    for x1,x2 in zip(grid_start, grid_end):

        plt.fill_between([x1,x2],[yrange[1],yrange[1]], color=greyval)

    # Switch to include plotting the last microlensing filter
    
    if show_microlensing_filt:
        n_show = len(filt_set)
    else:
        n_show = len(filt_set) - 1

    # Loop through filters
    for i in range(n_show):

        filt = A[i,:]
        
        # plot microlensing filter at half-height for visual clarity

        if show_microlensing_filt & (i == n_show-1):    
            filt = filt*0.5
            
        ax = plt.plot(wave, filt, linewidth=2, 
                 color=plt.cm.plasma(float(i)/7.0))
        plt.fill_between(wave, filt, 
                         alpha=0.5, 
                         color=plt.cm.plasma(float(i)/7.0))
        
    plt.xlabel('Wavelength (microns)', size=20)
    plt.xlim(wave_range)
    plt.ylim(yrange)
    
    ax = plt.gca()
    # ax.set_yticklabels([])
    
    # Kill the left, top and right axes
    ax = fig.add_subplot(1, 1, 1)
    ax.spines['top'].set_color('none')
    # ax.spines['left'].set_color('none')
    # ax.spines['right'].set_color('none')
    ax.tick_params(top='off', left='on', right='on')
    
    # add title, if requested
    if title != '':
        plt.annotate(title, fontsize=20, 
                     #xy=(0.5, 0.94), horizontalalignment='center',
                     xy=(0.95, 0.94), horizontalalignment='right',
                     xycoords = 'axes fraction')
    
    # add alternate axes showing redshifts
    
    Lya  = 0.1216
    CaHK = 0.4000
    Ha   = 0.6563

    # Cheat the plot down and add alternate top axis
    
    ax_shift_start = 15
    ax_shift = 60
    
    fig.subplots_adjust(top=0.7)

    newax = ax.twiny()
    newax.patch.set_visible(False)
    newax.xaxis.set_ticks_position('top')
    newax.xaxis.set_label_position('top')
    newax.spines['top'].set_position(('outward', ax_shift_start))
    newax.spines['left'].set_color('none')
    newax.spines['right'].set_color('none')

    newax.set_xlabel(r'Redshift (Ly$\alpha$)', size=15)
    newax.set_xlim(np.array(wave_range)/Lya - 1)

    newax2 = ax.twiny()
    newax2.patch.set_visible(False)
    newax2.xaxis.set_ticks_position('top')
    newax2.xaxis.set_label_position('top')
    newax2.spines['top'].set_position(('outward', ax_shift_start + ax_shift))
    newax2.spines['left'].set_color('none')
    newax2.spines['right'].set_color('none')

    newax2.set_xlabel(r'4000${\rm\AA}$ Break', size=15)
    newax2.set_xlim(np.array(wave_range)/CaHK - 1)

    newax3 = ax.twiny()
    newax3.patch.set_visible(False)
    newax3.xaxis.set_ticks_position('top')
    newax3.xaxis.set_label_position('top')
    newax3.spines['top'].set_position(('outward', ax_shift_start + 2.0*ax_shift))
    newax3.spines['left'].set_color('none')
    newax3.spines['right'].set_color('none')

    newax3.set_xlabel(r'Redshift (H$\alpha$)', size=15)
    newax3.set_xlim(np.array(wave_range)/Ha - 1)
    
    # save plot if requested
    if save_plot:
        print('Saving plot to',plotfilename)
        plt.savefig(plotfilename)

    return

if __name__ == '__main__':
    tmp = 3/2
    print(10*'\n'+'Python3: This should be 1.500 = %.3f\n' % tmp)
    import time
    tic = time.time()
    DoAll()
    tmp = time.time()-tic
    print('Completed in %.3f seconds\n' % tmp)

