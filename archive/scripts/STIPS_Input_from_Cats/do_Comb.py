#! /usr/bin/env python
'''
Ingest HST B-I optical catalog ascii files, then
create non-overlapping combined list. To Do: 
1. Make it generic, for arbitrary number of fields
2. Combine dev scripts, ingesting fits tables
'''
def DoAll():
    from astropy.io import ascii
    from astropy.table import Table

    import numpy as np

    tol = 1.5e-5
    
    # Read all files, separately
    F3 = ascii.read('M83-F3.st5.txt')
    F4 = ascii.read('M83-F4.st5.txt')
    F5 = ascii.read('M83-F5.st5.txt')
    F6 = ascii.read('M83-F6.st5.txt')
    F7 = ascii.read('M83-F7.st5.txt')
    F8 = ascii.read('M83-F8.st5.txt')
    F9 = ascii.read('M83-F9.st5.txt')

    in3,in5,in8,in9 = np.arange(F3['ra'].size),np.arange(F5['ra'].size),np.arange(F8['ra'].size),np.arange(F9['ra'].size)

    fl3,fl5,fl8,fl9 = np.zeros_like(in3),np.zeros_like(in5),np.zeros_like(in8),np.zeros_like(in9)

    fl9 = updateFlag(fl9,tol,F9['ra'],F9['dec'],F7['ra'],F7['dec'],np)
    fl9 = updateFlag(fl9,tol,F9['ra'],F9['dec'],F3['ra'],F3['dec'],np)
    fl9 = updateFlag(fl9,tol,F9['ra'],F9['dec'],F8['ra'],F8['dec'],np)
    fl9 = updateFlag(fl9,tol,F9['ra'],F9['dec'],F5['ra'],F5['dec'],np)
    fl9 = updateFlag(fl9,tol,F9['ra'],F9['dec'],F4['ra'],F4['dec'],np)
    fl9 = updateFlag(fl9,tol,F9['ra'],F9['dec'],F6['ra'],F6['dec'],np)

    fl5 = updateFlag(fl5,tol,F5['ra'],F5['dec'],F4['ra'],F4['dec'],np)
    fl5 = updateFlag(fl5,tol,F5['ra'],F5['dec'],F6['ra'],F6['dec'],np)
    fl5 = updateFlag(fl5,tol,F5['ra'],F5['dec'],F3['ra'],F3['dec'],np)
    fl5 = updateFlag(fl5,tol,F5['ra'],F5['dec'],F8['ra'],F8['dec'],np)

    fl3 = updateFlag(fl3,tol,F3['ra'],F3['dec'],F6['ra'],F6['dec'],np)
    fl3 = updateFlag(fl3,tol,F3['ra'],F3['dec'],F7['ra'],F7['dec'],np)

    fl8 = updateFlag(fl8,tol,F8['ra'],F8['dec'],F4['ra'],F4['dec'],np)
    fl8 = updateFlag(fl8,tol,F8['ra'],F8['dec'],F7['ra'],F7['dec'],np)

    in30,in50,in80,in90 = in3[fl3==0],in5[fl5==0],in8[fl8==0],in9[fl9==0]

    ra = np.concatenate([F4['ra'],F6['ra'],F7['ra'],F3['ra'][in30[:]],F5['ra'][in50[:]],F8['ra'][in80[:]],F9['ra'][in90[:]]])
    dec = np.concatenate([F4['dec'],F6['dec'],F7['dec'],F3['dec'][in30[:]],F5['dec'][in50[:]],F8['dec'][in80[:]],F9['dec'][in90[:]]])
    m1 = np.concatenate([F4['m1'],F6['m1'],F7['m1'],F3['m1'][in30[:]],F5['m1'][in50[:]],F8['m1'][in80[:]],F9['m1'][in90[:]]])
    err1 = np.concatenate([F4['err1'],F6['err1'],F7['err1'],F3['err1'][in30[:]],F5['err1'][in50[:]],F8['err1'][in80[:]],F9['err1'][in90[:]]])    
    m2 = np.concatenate([F4['m2'],F6['m2'],F7['m2'],F3['m2'][in30[:]],F5['m2'][in50[:]],F8['m2'][in80[:]],F9['m2'][in90[:]]])
    err2 = np.concatenate([F4['err2'],F6['err2'],F7['err2'],F3['err2'][in30[:]],F5['err2'][in50[:]],F8['err2'][in80[:]],F9['err2'][in90[:]]])

    tab = [ra,dec,m1,err1,m2,err2]
    nms = ('ra','dec','m1','err1','m2','err2')
    fmt = {'ra':'%10.5f', 'dec':'%10.5f', 'm1':'%8.3f', 'err1':'%8.3f', 'm2':'%8.3f', 'err2':'%8.3f'}
    t = Table(tab, names=nms)

    ascii.write(t, 'M83-Comb0.txt', format='fixed_width', delimiter='', formats=fmt)


def updateFlag(flag,tol,ra1,dec1,ra2,dec2,np):
    r1min,r1max,d1min,d1max,r2min,r2max,d2min,d2max = np.amin(ra1)-2*tol,np.amax(ra1)+2*tol,np.amin(dec1)-2*tol,np.amax(dec1)+2*tol,\
	np.amin(ra2)-2*tol,np.amax(ra2)+2*tol,np.amin(dec2)-2*tol,np.amax(dec2)+2*tol
    in1,in2 = [i for i in range(ra1.size) if ((flag[i]==0)&(r2min<ra1[i]<r2max)&(d2min<dec1[i]<d2max))],\
              [i for i in range(ra2.size) if ((r1min<ra2[i]<r1max)&(d1min<dec2[i]<d1max))]
    if (len(in1)>0):
    	fl1,ra1,dec1,ra2,dec2 = flag[in1[:]],ra1[in1[:]],dec1[in1[:]],ra2[in2[:]],dec2[in2[:]]
    	fl1 = np.array([fl1[i]+1 if (np.any(np.fabs(ra1[i]-ra2[np.fabs(dec1[i]-dec2)<tol])<tol)) else fl1[i] for i in range(len(fl1))])
    	flag[in1[:]] = fl1[:]
    return flag

    
if __name__ == '__main__':
    tmp = 3/2
    print('This should be 1.5: %.3f' % tmp)
    import time
    tic = time.time()
    DoAll()
    toc = time.time()
    print(toc-tic)
