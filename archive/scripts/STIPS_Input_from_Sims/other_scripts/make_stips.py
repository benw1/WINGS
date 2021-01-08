#! /usr/bin/env python
'''
Ingest catalogs with ra, dec and absolute AB mags (6 WFI notional 
bands) and produce STPS input files. Filter order: f_1 - f_6 >>> 
Z087,Y106,J129,H158,F184,W149. Example: 
===

>>> from make_stips import *
>>> DoAll(4.61,'test1.txt','M83')
Distance is d = 4.61 Mpc

Read in test1.txt

Wrote out M83_Z.tbl

Wrote out M83_Y.tbl

Wrote out M83_J.tbl

Wrote out M83_H.tbl

Wrote out M83_F.tbl

Wrote out M83_W.tbl

===
where test1.txt is formatted as:
  204.21051   -29.81816    -8.471    -7.409    -8.471    -7.409    -8.471    -7.409
  204.23604   -29.80238    -9.855    -9.257    -9.855    -9.257    -9.855    -9.257
  ...
  ...
'''
import numpy as np
from astropy.io import ascii
from astropy.table import Table
from astropy import constants as const

ZP_AB = np.array([26.365,26.357,26.320,26.367,25.913,27.480])

def DoAll(d=10,infile='file1.txt',outprefix='Halo'):
    print('Distance is d = %4.2f Mpc\n' % d)
    u = 25+5*np.log10(d)

    data = ascii.read(infile)
    RA, DEC, M1, M2, M3, M4, M5, M6 = data['col1'], data['col2'], data['col3'], data['col4'],\
				      data['col5'], data['col6'], data['col7'], data['col8']
    print('Read in %s \n' % infile)
    M = np.array([M1,M2,M3,M4,M5,M6]).T
    m = M+u    
    N = get_Counts(m,ZP_AB)

    write_stips(RA,DEC,N[:,0],outprefix+'_Z.tbl')
    write_stips(RA,DEC,N[:,1],outprefix+'_Y.tbl')
    write_stips(RA,DEC,N[:,2],outprefix+'_J.tbl')
    write_stips(RA,DEC,N[:,3],outprefix+'_H.tbl')
    write_stips(RA,DEC,N[:,4],outprefix+'_F.tbl')
    write_stips(RA,DEC,N[:,5],outprefix+'_W.tbl')
    return

def get_Counts(m,ZP):
    return 10**((m-ZP)/(-2.5))

def write_stips(ra,dec,counts,outfile):
    id  = np.arange(ra.size)+1
    ones = np.ones_like(id)
    typ  = np.repeat(np.array(['point']),id.size)
    cmnt = np.repeat(np.array(['comment']),id.size)
    tab  = [id, ra, dec, counts, typ, ones, ones, ones, ones, cmnt]
    nms  = ('id', 'ra', 'dec', 'flux', 'type', 'n', 're', 'phi', 'ratio', 'notes')
    fmt  = {'id':'%10d', 'ra':'%12.5f', 'dec':'%12.5f', 'flux':'%15.5f', 'type':'%8s', \
            'n':'%8.1f', 're':'%8.1f', 'phi':'%8.1f', 'ratio':'%8.1f', 'notes':'%8s'}
    t    = Table(tab, names=nms)
    ascii.write(t, outfile, format='fixed_width', delimiter='', formats=fmt)
    print('Wrote out %s \n' % outfile)
    return

if __name__ == '__main__':
    tmp = 3/2
    print('\n Python3: This should be 1.500 = %.3f\n' % tmp)
    import time
    tic = time.time()
    DoAll()
    tmp = time.time()-tic
    print('Completed in %.3f seconds\n' % tmp)
