#! /usr/bin/env python
'''
Ingest HST B-I optical catalog ascii files, then
create non-overlapping combined lists
'''
def DoAll():
    from astropy.io import ascii
    from astropy.table import Table
    import numpy as np
    
    tol = 1.5e-5    
    F4 = ascii.read('M83-F4.st5.txt')
    F8 = ascii.read('M83-F8.st5.txt')

    out1 = np.arange(F4['ra'].size)
    out2 = matchLists2(tol,F4['ra'],F4['dec'],F8['ra'],F8['dec'],np)

    tab = [out1,out2]
    nms = ('index1','index2')
    fmt = {'index1':'%10d', 'index2':'%10d'}
    t = Table(tab, names=nms)

    ascii.write(t, 'indexCombX.txt', format='fixed_width', delimiter='', formats=fmt)


def matchLists1(tol,ra1,dec1,ra2,dec2,np):
    tol2 = tol**2
    in2 = np.arange(len(ra2))
    in1 = np.array([in2[np.argmin((ra1[i]-ra2)**2 + (dec1[i]-dec2)**2)] for i in range(len(ra1))])
    in1 = np.array([in1[i] if (((ra1[i]-ra2[in1[i]])**2+(dec1[i]-dec2[in1[i]])**2)<tol2) else -1  for i in range(len(ra1))])
    return in1


def matchLists2(tol,ra1,dec1,ra2,dec2,np):
    d1,d2 = np.empty((ra1.size, 2)), np.empty((ra2.size, 2))
    d1[:,0],d1[:,1],d2[:,0],d2[:,1] = ra1,dec1,ra2,dec2
    in1 = getindex(d1,d2,tol)
    in1[in1==ra2.size] = -1
    return in1


def getindex(d1, d2, r):
    from scipy.spatial import cKDTree
    t = cKDTree(d2)
    d, idx = t.query(d1, distance_upper_bound=r)
    return idx
    
if __name__ == '__main__':
    tmp = 3/2
    print('This should be 1.5: %.3f' % tmp)
    import time
    tic = time.time()
    DoAll()
    toc = time.time()
    print(toc-tic)
