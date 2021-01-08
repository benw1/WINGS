#!/usr/bin/env python
from astropy.io import fits
import numpy as np
from astropy.convolution import convolve,  Box2DKernel

infile='ch01s'
outfile= ['sky_X','sky_Z','sky_Y','sky_H'] # 'tmp06s.fits'
wv = [0.606,0.87,1.06,1.58]

# Converts MJy/Sr to uJy/pixel for 0.75"/px
C = 13.22

boxsize=2
n=7

lo,hi=1e3,5e4

def DoAll(infile=infile,outfile=outfile,boxsize=boxsize,n=n,lo=lo,hi=hi):
    hdulist = fits.open(infile+'.fits')
    a = hdulist['PRIMARY'].data
    
    a = smooth_iter(a,boxsize,n,lo,hi)

    a =	((a-1000)/1000)*C*1e-6

    hdulist[0].header['BUNIT'] = 'Jy/pixel'

    for i,out in enumerate(outfile):        
        hdulist['PRIMARY'].data = a *(wv[i]/3.6)   
        hdulist.writeto(out+'.fits')
    return

def smooth_iter(a,boxsize,n,lo,hi):
    a = chop_once(a,lo,hi)
    box_2D_kernel = Box2DKernel(boxsize)
    
    for  i in range(n):
        a = convolve(a, box_2D_kernel)
        a = chop_once(a,lo,hi)
    return a

def chop_once(a,lo,hi):
    for i in range(a.shape[0]):
        a[i,][a[i,]<lo] = lo
        a[i,][a[i,]>hi] = hi
    return a


if __name__ == '__main__':
    tmp = 3/2
    print(10*'\n'+'Python3: This should be 1.500 = %.3f\n' % tmp)
    import time
    tic = time.time()
    DoAll()
    tmp = time.time()-tic
    print('Completed in %.3f seconds\n' % tmp)
