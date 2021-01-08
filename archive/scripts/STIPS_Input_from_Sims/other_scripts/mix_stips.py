#! /usr/bin/env python
'''
Joins a stellar population simulation and a field galaxy 
sample to produce new STIPS input file. 

Usable as is for any two STIPS input files where the first 
is 'contaminated' by sample_size elements from the second.

Room for improvement:
1. Use coordinate transformation by creating 'fake' header 
info rather than reading in prior STIPS simulated image
2. Command line parameter input
3. Add STIPS 'internal' format header
4. Produce a more genrally useful mixing script

(... and obviously, document the code better)

'''
import numpy as np
from astropy.io import fits
from astropy.io import ascii
from astropy.table import Table
from astropy import wcs

filters   = ['Z','Y','J','H','F']
distances = [1,3,5,10]

starprefix  = 'h15_shell_'
galprefix = '1000_gals_'
outprefix = 'Mixed_Shell_'

# Prior simulation /w starfile for wcs info
imfiles   = ['sample01.fits','sample03.fits','sample05.fits','sample10.fits']

# How many contaminating galaxies to add
sample_size = 5000

def DoAll(filters=['H'],distances=[5],imfiles=['sample05.fits'],sample_size=500,
          starprefix='h15_shell_',galprefix='1000_gals_',outprefix='Mixed_Shell_'):
    for i,dist in enumerate(distances):
        radec   = rand_radec(imfiles[i],sample_size)
        in1 = []
        for j,filt in enumerate(filters):
            galfile  = galprefix+filt+'.tbl'
            galaxies,in1 = sample_galaxies(radec,galfile,in1)
            starfile = starprefix+str(dist)+'Mpc_'+filt+'.tbl'
            joined   = join_lists(galaxies,starfile)
            outfile  = outprefix+str(distances[i])+'Mpc_'+filters[j]+'.tbl'
            write_stips(joined,outfile)
    return


def rand_radec(imfile,n):
    sky = fits.open(imfile)
    print('\nGot coordinates from %s \n' % imfile)
    w = wcs.WCS(sky[1].header)
    xy = np.random.rand(n,2)*(sky[1].data).shape
    np.random.shuffle(xy)
    return w.wcs_pix2world(xy,1)


def sample_galaxies(radec,galfile,in1):
    gals  = ascii.read(galfile)
    print('Picked galaxies from %s ' % galfile)
    gals  = np.array([gals['flux'],gals['type'],gals['n'],\
                      gals['re'],gals['phi'],gals['ratio']]).T
    if len(in1)==0:
        print('\n\n NEW SAMPLE \n\n')
        in1 = np.random.randint(0,gals.shape[0],radec.shape[0])
    gals = gals[in1]
    return np.concatenate((radec,gals),axis=1),in1


def join_lists(gals,starfile):
    stars  = ascii.read(starfile)
    print('Got stellar population from %s ' % starfile)
    stars = np.array([stars['ra'],stars['dec'],stars['flux'],stars['type'],\
                      stars['n'],stars['re'],stars['phi'],stars['ratio']]).T
    mix = np.concatenate((stars,gals),axis=0)
    ID  = np.arange(mix.shape[0])+1
    cmnt = np.repeat(np.array(['comment']),ID.size)
    return [ID,mix[:,0].astype(float),mix[:,1].astype(float),mix[:,2].astype(float),mix[:,3],\
           mix[:,4].astype(float),mix[:,5].astype(float),mix[:,6].astype(float),\
           mix[:,7].astype(float),cmnt]


def write_stips(tab,outfile):
    nms  = ('id', 'ra', 'dec', 'flux', 'type', 'n', 're', 'phi', 'ratio', 'notes')
    fmt  = {'id':'%10d', 'ra':'%15.7f', 'dec':'%15.7f',  'flux':'%15.7f',  'type':'%8s',\
            'n':'%3.1f', 're':'%15.7f', 'phi':'%15.7f', 'ratio':'%15.7f', 'notes':'%8s'}
    t    = Table(tab, names=nms)
    ascii.write(t, outfile, format='fixed_width', delimiter='', formats=fmt)
    print('Wrote out %s \n' % outfile)
    return

if __name__ == '__main__':
    tmp = 3/2
    print('\n Python3: This should be 1.500 = %.3f\n' % tmp)
    import time
    tic = time.time()
    DoAll(filters,distances,imfiles,sample_size,starprefix,galprefix,outprefix)
    tmp = time.time()-tic
    print('Completed in %.3f seconds\n' % tmp)
