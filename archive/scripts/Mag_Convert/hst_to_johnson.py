#! /usr/bin/env python
'''
Reads in HST photometry fits files, converts HST Vega mags to 
Johnson Vega mags, and writes separate ASCII files with HST 
and Johnson mags. Currently hardcoded to work with UVIS/ACS 
F475W,F625W,F775W and Johnson BVR filters.

Usage:
  ./script.py filename1.fits filename2.fits ...
Example1:
  ./hst_to_johnson.py 14266_NGC6946BH1.gst.fits 14266_NGC6946BH1.st.fits
Example2:
  ls *.fits | xargs ./hst_to_johnson.py
'''
from __future__ import (absolute_import, division, print_function, unicode_literals)
import numpy as np
from astropy.io import fits
from astropy.io import ascii
from astropy.table import Table
from astropy import constants as const
c = const.c.cgs.value

filt = ['F475W','F625W','F775W']

def DoAll(filenames):
    for filename in filenames:
        print('\nReading in:\t\t', filename)
        X,Y,RA,DEC,vega_mags,mag_errors,detector = read_phot_fits_table(filename)

	# Zero flux density (Jy) & lambda_effective (cm); F475W, F625W, F775W
	# http://svo2.cab.inta-csic.es/theory/fps/index.php
        if 'UVIS' in detector:
            fo1 = np.array([3941.65,3132.88,2540.65])
            wv1 = np.array([0.4697,0.615564,0.758807])*1e-4
        else: # Assume ACS
            fo1 = np.array([3931.74,3079.87,2513.31])
            wv1 = np.array([0.470819,0.626619,0.765263])*1e-4

	# Johnson B, V, R; Bessell et al. (1998)
        fo2 = np.array([4063,3636,3064])
        wv2 = np.array([0.438,0.545,0.641])*1e-4

        # Exclude unless all three bands have mag<30
        m1 = np.array(vega_mags).T
        err = np.array(mag_errors).T
        in1 = np.arange(len(X))
        in1 = in1[(m1[:,0]<30)&(m1[:,1]<30)&(m1[:,2]<30)]
        X,Y,RA,DEC,m1,err = X[in1],Y[in1],RA[in1],DEC[in1],m1[in1,:],err[in1,:]

        # Flux density in ergs/s/cm2/Hz > ergs/s/cm2/cm
        f_nu1 = 10**(m1/(-2.5)) * fo1 * 1e-23
        f_lm1 = np.log10(f_nu1*c/wv1**2)
        
        wv1, wv2 = np.log10(wv1),np.log10(wv2)
        f_lm2 = np.zeros_like(f_lm1)

        tmp = np.array([np.polyfit(wv1[0:2], [f_lm1[i,0], f_lm1[i,1]], 1) for i in range(f_nu1.shape[0])])
        A, B = tmp[:,0], tmp[:,1]
        f_lm2[:,0], f_lm2[:,1] = A*wv2[0]+B, A*wv2[1]+B
        del tmp, A, B

        tmp = np.array([np.polyfit(wv1[1:3], [f_lm1[i,1], f_lm1[i,2]], 1) for i in range(f_nu1.shape[0])])
        A, B = tmp[:,0], tmp[:,1]
        f_lm2[:,2] = A*wv2[2]+B
        del tmp, A, B

        f_nu2 = (10**f_lm2) * ((10**wv2)**2) * 1e23 / c        
        m2 = -2.5*np.log10(f_nu2/fo2)

        nms = ('X','Y','RA','DEC','m_B','err_B','m_V','err_V','m_R','err_R')
        fmt = {'X':'%10.3f', 'Y':'%10.3f', 'RA':'%15.7f', 'DEC':'%15.7f', 'm_B':'%10.3f', 'err_B':'%10.3f',\
               'm_V':'%10.3f', 'err_V':'%10.3f','m_R':'%10.3f', 'err_R':'%10.3f'}
        tab1 = [X,Y,RA,DEC,m1[:,0],err[:,0],m1[:,1],err[:,1],m1[:,2],err[:,2]]
        tab2 = [X,Y,RA,DEC,m2[:,0],err[:,0],m2[:,1],err[:,1],m2[:,2],err[:,2]]
        outfile1 = '.'.join(filename.split('.')[:-1])+'_hst.txt'
        outfile2 = '.'.join(filename.split('.')[:-1])+'_bvr.txt'
        t1,t2 = Table(tab1, names=nms), Table(tab2, names=nms)
        ascii.write(t1, outfile1, format='fixed_width', delimiter='', formats=fmt, overwrite=True)
        print('Wrote out:\t\t', outfile1)
        ascii.write(t2, outfile2, format='fixed_width', delimiter='', formats=fmt, overwrite=True)
        print('\t\t\t', outfile2)
        return
    
def read_phot_fits_table(filename):
    photTable = fits.open(filename)
    detector = photTable[0].header['CAMERA']
    data = photTable[1].data; del photTable
    vega_mags  = [data[filt[0]+'_VEGA'], data[filt[1]+'_VEGA'], data[filt[2]+'_VEGA']]
    mag_errors = [data[filt[0]+'_ERR'], data[filt[1]+'_ERR'], data[filt[2]+'_ERR']]
    X,Y,RA,DEC = data['X'],data['Y'],data['RA'],data['DEC']
    return X,Y,RA,DEC,vega_mags,mag_errors,detector

if __name__ == '__main__':
    import sys, time
    tic = time.time()
    print('\n\nPython check: This should be 1.500 = %.3f \n' % (3/2))
    DoAll(sys.argv[1:])
    print('\n\nCompleted in %.3f seconds \n' % (time.time()-tic))
