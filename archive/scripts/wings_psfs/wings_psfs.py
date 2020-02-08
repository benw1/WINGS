#!/usr/bin/env python
'''
Produces PSF fits files for WFI chips both at native pixel scale 
and at an oversampled pixel-scale. 

This script is executable from the command line (POSIX shell) and 
the functions can be imported/run from the Python interpreter.

Requires the WebbPSF & PySynphot packages plus associated data files.

Usage:

# from shell (Change values at line 116-ish first)
./wings_psfs.py

# from interpreter
from wings_psfs import write_psf
write_psf(det='SCA01',filt='Z087',pos=(2048,2048),oversample=10,outprefix='WFI')

from wings_psfs import all_psf, chips, filternames
all_psf(chips=[chips[0]],filternames=filternames,grid=5,oversample=10,outprefix='WFI')

----------------------------
Last update on: 05/08/2017
Report bugs to: rubab@uw.edu
'''
assert 3/2 == 1.5, '\n\nNot using Python3 is a \t\t terrible \t horrible \t no good \t very bad idea.\n'
import concurrent.futures
import numpy as np
from webbpsf.wfirst import WFI
from os import cpu_count

''' List of WFI chips to produce PSFs for, e.g., SCA01 ... SCA18'''
chips = ['SCA01','SCA02','SCA03', 'SCA04','SCA05','SCA06',\
         'SCA07','SCA08','SCA09', 'SCA10','SCA11','SCA12',\
         'SCA13','SCA14','SCA15','SCA16','SCA17','SCA18']


''' New 'Blue' filter not supported yet'''
filternames = ['Z087','Y106','J129','H158','F184']


''' Sample the PSF at NxN positions on the chip'''
grid = 11


''' Oversample by this factor the native pixel scale of 0.11"'''
oversample = 11


'''Create CPU and I/O bound pools'''
if __name__ != '__main__':
    cpu_pool = concurrent.futures.ThreadPoolExecutor(max_workers=cpu_count())
else:
    cpu_pool = concurrent.futures.ProcessPoolExecutor(max_workers=cpu_count())
io_pool  = concurrent.futures.ThreadPoolExecutor(max_workers=4*cpu_count())


def all_psf(chips=['SCA01'],filternames=['Z087'],grid=1,oversample=oversample,outprefix='WFI'):
    ''' Generates list of WFI PSFs to produce and submits write_psf() jobs to 
        CPU bound thread.'''
    positions = get_positions(grid)
    det,filt,pos = [],[],[]
    for _det in chips:
        for _filt in filternames:
            for _pos in positions:
                det.append(_det); filt.append(_filt); pos.append(_pos)
    cpu_pool.map(write_psf,det,filt,pos,chunksize=int(np.ceil(len(det)/cpu_count())))
    '''Necessary if running from interpreter'''
    for t in io_pool._threads:
        t.join()
    return None


def write_psf(det='SCA01',filt='Z087',pos=(2048,2048),oversample=11,outprefix='WFI'):
    ''' Produces WFI PSFs with both native pixel-scale and an oversampled one at 
        the given position of the named detector (WFI chip) for the requested filter, 
        and then submits write_fits() jobs to I/O bound threads'''
    wfi=WFI()
    outpre = '_'.join([outprefix,det,filt,'pos',str(pos[0]),str(pos[1])])
    outfile0 = '_'.join([outpre,str(oversample)+'x','oversampled.fits'])
    outfile1 = '_'.join([outpre,'native_pixel.fits'])
    wfi.detector = det
    wfi.detector_position = pos
    wfi.filter = filt
    psf = wfi.calc_psf(oversample=oversample)
    io_pool.submit(write_fits,psf[0],outfile0)
    io_pool.submit(write_fits,psf[1],outfile1)
    return None



def write_fits(hdu,outfile):
    ''' Writes HDUlist object to FITS file'''
    return hdu.writeto(outfile,overwrite=True)


def get_positions(grid=5):
    ''' Returns NxN grid positions on 4K WFI chips'''
    if grid<2:
        return [(2048,2048)]
    else:
        x0 = np.floor(4096/(grid+1))
        x1 = np.floor(x0*grid)
        xcen,ycen = np.linspace(x0,x1,grid), np.linspace(x0,x1,grid)
        positions = []
        for i in range(grid):
            for j in range(grid):
                positions.append((int(xcen[i]),int(ycen[j])))
        return positions

    
'''If executed from POSIX shell'''
if __name__ == '__main__':
    import time
    tic = time.time()
    all_psf([chips[0],chips[5],chips[-1]],filternames,grid,oversample)
    #all_psf(chips=['SCA01'],filternames=['Z087','H158'],grid=3)
    cpu_pool.shutdown(wait=True)
    io_pool.shutdown(wait=True)
    print('\n\nCompleted in %.3f seconds \n' % (time.time()-tic))
