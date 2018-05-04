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
filenames = ['h15.shell.10Mpc.in', 'h15.shell.1Mpc.in', 'h15.shell.3Mpc.in', 'h15.shell.5Mpc.in']
filternames=['Z','Y','J','H','F','W']


def DoAll():
    return [make_stips(int(infile.split('.')[2][:-3]),\
                       infile,\
                       '_'.join(infile.split('.')[:-1]))\
            for infile in filenames]
        

def make_stips(d=10,infile='file1.txt',outprefix='Halo'):
    print('\nDistance is d = %4.2f Mpc\n' % d)
    u = 25+5*np.log10(d)

    data = ascii.read(infile)
    RA, DEC, M1, M2, M3, M4, M5, M6 = \
        data['col1'], data['col2'], data['col3'], data['col4'],\
        data['col5'], data['col6'], data['col7'], data['col8']
    print('\nRead in %s \n' % infile)
    
    M = np.array([M1,M2,M3,M4,M5,M6]).T
    m = M+u    
    N = get_Counts(m,ZP_AB)

    return [write_stips(RA,DEC,N[:,i],outprefix+'_'+filtername+'.tbl')\
            for i,filtername in enumerate(filternames)]


def get_Counts(m,ZP):
    return 10**((m-ZP)/(-2.5))


def write_stips(ra,dec,counts,outfile):
    ID  = np.arange(ra.size)+1
    ones = np.ones_like(ID)
    typ  = np.repeat(np.array(['point']),ID.size)
    cmnt = np.repeat(np.array(['comment']),ID.size)
    
    tab  = [ID, ra, dec, counts, typ, ones, ones, ones, ones, cmnt]
    nms  = ('id', 'ra', 'dec', 'flux', 'type', 'n', 're', 'phi', 'ratio', 'notes')
    t    = Table(tab, names=nms)

    fmt = ('%10d','%15.7f','%15.7f','%15.7f','%8s','%10.3f', '%15.7f','%15.7f','%15.7f','%8s')    
    ascii.write(t, outfile, format='fixed_width', delimiter='', formats=dict(zip(nms,fmt)))
    
    return print('Wrote out %s \n' % outfile)


if __name__ == '__main__':
    import time
    tic = time.time()
    assert 3/2 == 1.5, 'Not running Python3 may lead to wrong results'
    DoAll()
    print('\n\nCompleted in %.3f seconds \n' % (time.time()-tic))
