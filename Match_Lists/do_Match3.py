#! /usr/bin/env python
'''
Matches coordinates from two files within given matching radius.
Assumes first two columns of each input file contains RA-DEC in 
degrees and matching radius in arcsecs. Writes out matching 
indices and coordinates.

Command line usage:
  ./script.py filename1 filename2 matching_radius
Example:
  ./match_lists.py test1.txt test2.txt 0.05

Interactive usage:
  from script import *
  DoAll(filename1,filename2,matching_radius)
Example:
 from match_lists import *
 DoAll(test1.txt,test2.txt,0.05)


To Do:
1. Handle files with index column
2. Match lists of pixels
3. Retrieve other useful columns
4. Replace 'match_xy' and 'match_radec' functionalities


'''
from __future__ import (absolute_import, division, print_function, unicode_literals)
from astropy.io import ascii
from astropy.table import Table
import numpy as np
from scipy.spatial import cKDTree

# Main function
def DoAll(infile1,infile2,tol):
    print('\nMatching radius: \t %f arcsecs' % tol)
    print('Reading in: \t\t',infile1, infile2)

    tol, data1, data2 = tol/3600, ascii.read(infile1), ascii.read(infile2)
    ra1,dec1,ra2,dec2 = data1['col1'],data1['col2'],data2['col1'],data2['col2']
    out1,out2 = np.arange(len(ra1)), matchLists(tol,ra1,dec1,ra2,dec2)
    out1,out2 = out1[out2!=-1], out2[out2!=-1]

    print('Number of stars:\t', len(ra1), len(ra2))
    print('Number of matches:\t', out2.size)

    if ((out1.size>0)&(out2.size>0)&(out1.size==out2.size)):
        tab = [out1,out2,ra1[out1],dec1[out1],ra2[out2],dec2[out2]]
        nms = ('INDEX1','INDEX2','RA1','DEC1','RA2','DEC2')
        fmt = {'INDEX1':'%10d', 'INDEX2':'%10d', 'RA1':'%15.7f', 'DEC1':'%15.7f', 'RA2':'%15.7f', 'DEC2':'%15.7f'}
        write_out(tab,nms,fmt,infile1,infile2)
    else:
        print('\nNothing to write out')
    return

# Quick match; returns index of 2nd list coresponding to position in 1st
def matchLists(tol,ra1,dec1,ra2,dec2):
    d1,d2 = np.empty((ra1.size, 2)), np.empty((ra2.size, 2))
    d1[:,0],d1[:,1],d2[:,0],d2[:,1] = ra1,dec1,ra2,dec2
    t = cKDTree(d2)
    tmp, in1 = t.query(d1, distance_upper_bound=tol); del tmp
    in1[in1==ra2.size] = -1
    return in1

# Writes file with matching indices and coordinates
def write_out(tab,nms,fmt,infile1,infile2):
    outfile = 'Matched'+'_'+infile1.split('.')[0]+'_'+infile2.split('.')[0]+'.txt'
    t = Table(tab, names=nms)
    ascii.write(t, outfile, format='fixed_width', delimiter='', formats=fmt, overwrite=True)
    return print('Wrote out: \t\t', outfile)

# Slow match, shows what quick-match is doing
def matchLists2(tol,ra1,dec1,ra2,dec2):
    tol2 = tol**2
    in2 = np.arange(len(ra2))
    in1 = np.array([in2[np.argmin((ra1[i]-ra2)**2 + (dec1[i]-dec2)**2)] for i in range(len(ra1))])
    return np.array([in1[i] if (((ra1[i]-ra2[in1[i]])**2+(dec1[i]-dec2[in1[i]])**2)<tol2) else -1  for i in range(len(ra1))])    

# Execute if run from command line
if __name__ == '__main__':
    import sys, time
    tic = time.time()
    print('\n\nPython check: This should be 1.500 = %.3f \n' % (3/2))
    DoAll(str(sys.argv[1]),str(sys.argv[2]),float(sys.argv[3]))
    print('\n\nCompleted in %.3f seconds \n' % (time.time()-tic))
