#! /usr/bin/env python
'''
Reads in HST F606/F814/F125/F160 AB mags and produces STIPS
simulation input files for WFI filters
'''
from __future__ import (absolute_import, division, print_function, unicode_literals)

import numpy as np
from astropy.io import ascii
from astropy.table import Table
from astropy import constants as const

c = const.c.cgs.value
h = const.h.cgs.value
fo = 3631 # Jy

hst_filt = ['F606W','F814W','F125W','F160W']                                   # HST, ACS/ACS/WFC3/WFC3 filters
wfi_filt = ['X606','X625','X670','Z087','Y106','J129','H158','F184']           # WFI, 3 blue filter options

wv1 = np.array([0.599548,0.811524,1.236464,1.537033])*1e-4                     # lambda_pivot (cm), HST
wv2 = np.array([0.5965,0.6254,0.6725,0.8709,1.0611,1.2945,1.5809,1.8452])*1e-4 # same ... WFI
awp = np.array([0.7458,0.9712,0.8779,0.5419,0.5353,0.5054,0.5222,0.3658])*1e4  # Aeff*Weff/lambda_pivot (cm^2)

def DoAll(wv1,wv2):
    glxs, mags = [],[]
    for i,filt in enumerate(hst_filt):
        glxs.append(ascii.read(filt+'.dat'))
        mags.append(glxs[i]['mag'])
    m1  = np.array(mags).T

    in1 = np.arange(m1.shape[0])\
          [~(np.isnan(m1[:,0])|np.isnan(m1[:,1])|np.isnan(m1[:,2])|np.isnan(m1[:,3]))]

    # temporary cut at H=20
    in1 = in1[m1[in1,3]>20]

    m1 = m1[in1,:]

    f_nu1 = 10**(m1/(-2.5)) * fo * 1e-23                                       # ergs/s/cm^2/Hz
    N_lm1 = np.log10(f_nu1/(wv1*h))                                            # Photons/s/cm^2/cm
    N_lm2 = np.zeros((N_lm1.shape[0],wv2.shape[0]))
    wv1, wv2 = np.log10(wv1),np.log10(wv2)

    tmp = np.array([np.polyfit(wv1[0:2], [N_lm1[i,0], N_lm1[i,1]], 1) \
                    for i in range(N_lm1.shape[0])])
    A, B = tmp[:,0], tmp[:,1]
    N_lm2[:,0], N_lm2[:,1], N_lm2[:,2] = A*wv2[0]+B, A*wv2[1]+B, A*wv2[2]+B    # F606/814 >>> X606/625/670
    del A, B, tmp

    tmp = np.array([np.polyfit(wv1[1:3], [N_lm1[i,1], N_lm1[i,2]], 1) \
                    for i in range(N_lm1.shape[0])])
    A, B = tmp[:,0], tmp[:,1]
    N_lm2[:,3], N_lm2[:,4] = A*wv2[3]+B, A*wv2[4]+B                            # F814/110 >>> Z087/Y106
    del A, B, tmp

    tmp = np.array([np.polyfit(wv1[2:4], [N_lm1[i,2], N_lm1[i,3]], 1) \
                    for i in range(N_lm1.shape[0])])
    A, B = tmp[:,0], tmp[:,1]
    N_lm2[:,5], N_lm2[:,6], N_lm2[:,7] = A*wv2[5]+B, A*wv2[6]+B, A*wv2[7]+B    # F110/160 >>> J129/H158/F184
    del A, B, tmp

    N = 10**N_lm2 * awp * 10**wv2                                              # Counts/s; WFI
    t = [0,0,0,1,2,2,3,3]

    return [write_stips_table(glxs[t[i]],N[:,i],wfi_filt[i],in1) for i in range(len(t))]

def write_stips_table(gals,flux,filt,in1):
    tab = [gals['id'][in1], gals['ra'][in1], gals['dec'][in1], flux,\
           gals['type'][in1], gals['n'][in1], gals['re'][in1],\
           gals['phi'][in1], gals['ratio'][in1], gals['notes'][in1]]
    nms  = ('id', 'ra', 'dec', 'flux', 'type', 'n', 're', 'phi', 'ratio', 'notes')
    fmt  = {'id':'%10d', 'ra':'%15.7f', 'dec':'%15.7f',  'flux':'%15.7f',  'type':'%8s',\
            'n':'%10.3f', 're':'%15.7f', 'phi':'%15.7f', 'ratio':'%15.7f', 'notes':'%8s'}
    t    = Table(tab, names=nms)
    outfile = '.'.join([filt,'txt'])
    radec  = np.mean(np.array([gals['ra'][in1],gals['dec'][in1]]).T, axis=0)
    ascii.write(t, outfile, format='fixed_width', delimiter='', formats=fmt)
    return print('\nWrote out:', outfile) #, ';\tCenter = ', radec)

if __name__ == '__main__':
    import sys, time
    tic = time.time()
    print('\n\nPython check: This should be 1.500 = %.3f \n' % (3/2))
    DoAll(wv1,wv2)
    print('\n\nCompleted in %.3f seconds \n' % (time.time()-tic))
