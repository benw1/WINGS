#! /usr/bin/env python
'''
Ingest HST B-I optical catalog ascii files, then 
use M31 and blackbody color lookup tables to produce 
possible F110 and F160 mags
'''
def DoAll():
    from astropy.io import ascii
    from astropy.table import Table

    import numpy as np

    data = ascii.read('M83-Comb1.txt')
    m1, err1, m2, err2, ra, dec = data['m1'], data['err1'], data['m2'], data['err2'], data['ra'], data['dec']
    del data
    
    C12 = m1-m2
    
    data  = ascii.read('BB_color2.txt')
    cc12,cc13,cc14 = data['c12'], data['c13'], data['c14']
    del data

    data  = ascii.read('M31_col_B08-F15.txt')
    c12,c13,c14 = data['c12'], data['c13'], data['c14']
    del data

    m3,m4 = np.zeros_like(m1),np.zeros_like(m1)

    in1 = [i for i in range(m1.size) if C12[i]<0.1]
    in2 = [i for i in range(m1.size) if C12[i]>0.1]

    tmp = np.array([(m1[in1[i]]-cc13[np.argmin(np.fabs(cc12-C12[in1[i]]))],m1[in1[i]]-cc14[np.argmin(np.fabs(cc12-C12[in1[i]]))]) for i in range(len(in1))])
    m3[in1[:]],m4[in1[:]] = [tmp[:,0][i] for i in range(tmp[:,0].size)],[tmp[:,1][i] for i in range(tmp[:,1].size)]
    del	tmp, in1

    tmp = np.array([(m1[in2[i]]-c13[np.argmin(np.fabs(c12-C12[in2[i]]))],m1[in2[i]]-c14[np.argmin(np.fabs(c12-C12[in2[i]]))]) for i in range(len(in2))])
    m3[in2[:]],m4[in2[:]] = [tmp[:,0][i] for i in range(tmp[:,0].size)],[tmp[:,1][i] for i in range(tmp[:,1].size)]
    del tmp, in2

    # DoFig(m1,m2,'cmd1.pdf')
    # DoFig(m2,m3,'cmd2.pdf')
    # DoFig(m3,m4,'cmd3.pdf')

    tab = [ra,dec,m1,m2,m3,m4]
    nms = ('ra','dec','m1','m2','m3','m4')
    fmt = {'ra':'%10.5f', 'dec':'%10.5f', 'm1':'%8.4f', 'm2':'%8.4f', 'm3':'%8.4f', 'm4':'%8.4f'}
    t = Table(tab, names=nms)

    ascii.write(t, 'M83-Comb1_4mags.txt', format='fixed_width', delimiter='', formats=fmt)

def DoFig(m1,m2,filename):
    import matplotlib.pyplot as plt

    plt.rc("font", size=10, family='serif', weight='bold')
    plt.rc("axes", labelsize=7, titlesize=20)
    plt.rc("xtick", labelsize=7.5)
    plt.rc("ytick", labelsize=7.5)
    plt.rc("legend", fontsize=10)

    fig1 = plt.figure(figsize = ((5,5)))
    fig1.suptitle('M83')

    m1m2 = m1-m2

    x,y = m1m2, m2
    plt.plot(x,y,'r.')
    plt.xlim(-1,3)
    plt.ylim(30,16)
    plt.xlabel('m1-m2',fontsize=10)
    plt.ylabel(r'm2',fontsize=10)
    plt.savefig(filename)


if __name__ == '__main__':
    tmp = 3/2
    print('This should be 1.5: %.3f' % tmp)
    DoAll()

