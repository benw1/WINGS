#! /usr/bin/env python
'''
Joins a stellar population simulation and a field galaxy 
sample to produce new STIPS input file. 

Usable as is for any two STIPS input files where the first 
is 'contaminated' by sample_size elements from the second.

Room for improvement:
1. Command line parameter input
2. Add STIPS 'internal' format header
3. Generally usable CLass library (make_stips,hst_to_wfi,mix_stips)

(... and obviously, document the code better)

'''
import numpy as np
from astropy import wcs
from astropy.io import fits, ascii
from astropy.table import Table

filternames   = ['Z087','Y106','J129','H158','F184']
starprefix  = ['h15_shell_1Mpc', 'h15_shell_3Mpc', 'h15_shell_5Mpc', 'h15_shell_10Mpc']
outprefix = 'Mixed'

def DoAll(filternames=filternames,starprefix=starprefix,outprefix=outprefix):
    galaxies = []
    for i,starpre in enumerate(starprefix):
        radec = []
        for j,filt in enumerate(filternames):
            starfile = starpre+'_'+filt[0]+'.tbl'
            stars = read_stars(starfile)
            if i==0:
                galfile='.'.join((filt,'txt'))
                galaxies.append(read_galaxies(galfile))
            if len(radec)==0:
                cent = [stars[:,0].astype(float).mean(),stars[:,1].astype(float).mean()]
                radec = rand_radec(cent,galaxies[j].shape[0])
            write_stips(prep_tab(np.vstack((stars,np.hstack((radec,galaxies[j]))))),\
                        outfile='_'.join((outprefix,starpre,filt[0]))+'.tbl')
    return None

def read_stars(starfile):
    stars = ascii.read(starfile)
    print('\nGot stellar population from %s \n' % starfile)
    return np.array([stars['ra'],stars['dec'],stars['flux'],stars['type'],\
                      stars['n'],stars['re'],stars['phi'],stars['ratio']]).T

def read_galaxies(galfile):
    gals  = ascii.read(galfile)
    print('Got galaxy population from %s \n' % galfile)
    return np.array([gals['flux'],gals['type'],gals['n'],\
                      gals['re'],gals['phi'],gals['ratio']]).T

def rand_radec(centers,n):
    w  = make_wcs(centers)
    xy = np.random.rand(n,2)*(4096,4096)
    return w.wcs_pix2world(xy,1)

def prep_tab(mix):
    ID  = np.arange(mix.shape[0])+1
    cmnt = np.repeat(np.array(['comment']),ID.size)
    return [ID,mix[:,0].astype(float),mix[:,1].astype(float),mix[:,2].astype(float),\
            mix[:,3],mix[:,4].astype(float),mix[:,5].astype(float),\
            mix[:,6].astype(float),mix[:,7].astype(float),cmnt]

def write_stips(tab,outfile):
    nms = ('id',  'ra',    'dec',   'flux',  'type', 'n',   're',    'phi',   'ratio', 'notes')
    t   = Table(tab, names=nms)
    fmt = ('%10d','%15.7f','%15.7f','%15.7f','%8s','%10.3f','%15.7f','%15.7f','%15.7f','%8s')
    ascii.write(t, outfile, format='fixed_width', delimiter='', formats=dict(zip(nms,fmt)))
    return print('Wrote out %s \n' % outfile)

def make_wcs(centers=[0,0],crpix1=2048,cdelt1=-0.11/3600,cunit1='deg',\
             ctype1='RA---TAN',ctype2='DEC--TAN',lonpole=180,latpole=24.333335,\
             equinox=2000.0,radesys='ICRS'):
    w = wcs.WCS()
    w.wcs.cdelt = [cdelt1,-1*cdelt1]
    w.wcs.crpix = [crpix1,crpix1]
    w.wcs.crval = centers
    w.wcs.cunit = [cunit1,cunit1]
    w.wcs.ctype = [ctype1,ctype2]
    w.wcs.lonpole = lonpole
    w.wcs.latpole = latpole
    w.wcs.radesys = radesys
    w.wcs.equinox = equinox
    return w

def fetch_wcs(imfile):
    print('Getting coordinates from %s \n' % imfile)
    return wcs.WCS(fits.open(imfile)[1].header)

if __name__ == '__main__':
    import time
    tic = time.time()
    assert 3/2 == 1.5, 'Not running Python3 may lead to wrong results'
    DoAll()
    print('\n\nCompleted in %.3f seconds \n' % (time.time()-tic))
