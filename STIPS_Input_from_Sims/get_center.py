#! /usr/bin/env python
'''
Read in a simulation, return mean RA and Dec

Going forward:
Give center for multiple WFI chips or a specific chip 
'''

import numpy as np
from astropy.io import ascii

simfile = ['1Mpc.tbl','3Mpc.tbl','5Mpc.tbl','10Mpc.tbl']

def DoAll():
    for sim in simfile:
        print_center(sim)
    return

def print_center(simfile):
    sim = ascii.read(simfile)
    radec  = np.array([sim['ra'].astype(float),sim['dec'].astype(float)]).T
    center = np.mean(radec,axis=0)
    print('Simulation center [RA DEC] =',center,'\n')
    return

if __name__ == '__main__':
    tmp = 3/2
    print('\n Python3: This should be 1.500 = %.3f\n' % tmp)
    import time
    tic = time.time()
    DoAll()
    tmp = time.time()-tic
    print('Completed in %.3f seconds\n' % tmp)
