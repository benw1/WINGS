#!/usr/bin/env python
import numpy as np
import matplotlib.pyplot as plt
from astropy.io import ascii
from astropy.io import fits
from astropy.table import Table

dist=6.8

pixscale=0.05

xref=2284
yref=2048

def DoAll(filename='14266_NGC6946BH1.gst',dist=dist,pixscale=pixscale,pctol=50,xref=xref,yref=yref):

    data = fits.open(filename+'.fits')[1].data
    RA, DEC, X, Y, m1, m2, err1, err2 = data['RA      '], data['DEC     '], \
                                  data['X       '], data['Y       '], \
                                  data['F438W_VEGA '], data['F606W_VEGA '], \
                                  data['F438W_ERR '], data['F606W_ERR ']
    arctol = pctol / (dist*1e6/206265)
    pixtol = arctol / pixscale
    pixtol2 = pixtol**2
    in1 = np.arange(len(X))
    in1 = in1[(np.fabs(X-xref)<pixtol) & (np.fabs(Y-yref)<pixtol)]
    RA, DEC, X, Y, m1, m2, err1, err2 = RA[in1], DEC[in1], X[in1], Y[in1], m1[in1], m2[in1], err1[in1], err2[in1]
    in2 = np.arange(len(X))
    in2 = in2[((X-xref)**2 + (Y-yref)**2)<pixtol2]
    RA, DEC, X, Y, m1, m2, err1, err2 = RA[in2], DEC[in2], X[in2], Y[in2], m1[in2], m2[in2], err1[in2], err2[in2]

    DoFig(m1,m2,filename+'_'+str(dist),-1.4,5.3,28.8,16.8)

    DoFile(RA,DEC,m1,m2,filename+'_'+str(dist),-1.4,5.3,28.8,16.8)

def DoFig(m1,m2,filename,x1,x2,y1,y2):

    plt.rc("font", size=10, family='serif', weight='bold')
    plt.rc("axes", labelsize=7, titlesize=20)
    plt.rc("xtick", labelsize=7.5)
    plt.rc("ytick", labelsize=7.5)
    plt.rc("legend", fontsize=10)

    fig1 = plt.figure(figsize = ((5,5)))
    fig1.suptitle(filename)

    m1m2 = m1-m2

    x,y = m1m2, m2
    plt.plot(x,y,'rx')
    plt.xlim(x1,x2)
    plt.ylim(y1,y2)
    plt.xlabel('m1-m2',fontsize=10)
    plt.ylabel(r'm2',fontsize=10)
    plt.savefig(filename+'.png')


def DoFile(ra,dec,m1,m2,filename,x1,x2,y1,y2):
    m1m2 = m1-m2
    in1 = np.arange(len(m1))
    in1 = [in1[i] for i in range(len(in1)) if((x1<m1m2[i]<x2)&(y2<m2[i]<y1))]

    ra,dec,m1,m2 =ra[in1],dec[in1],m1[in1],m2[in1]

    tab1 = [m1,m2]
    nms1 = ('m1','m2')
    fmt1 = {'m1':'%8.3f', 'm2':'%8.3f'}
    t1 = Table(tab1, names=nms1)
    ascii.write(t1, filename+'.txt', format='fixed_width', delimiter='', formats=fmt1)

    tab2 = [ra,dec,m1,m2]
    nms2 = ('ra','dec','m1','m2')
    fmt2 = {'ra':'%15.8f', 'dec':'%15.8f', 'm1':'%8.3f', 'm2':'%8.3f'}
    t2 = Table(tab2, names=nms2)
    ascii.write(t2, filename+'.radec', format='fixed_width', delimiter='', formats=fmt2)


if __name__ == '__main__':
    tmp = 3/2
    print(10*'\n'+'Python3: This should be 1.500 = %.3f\n' % tmp)
    import time
    tic = time.time()
    DoAll(filename='14266_NGC6946BH1.gst')
    # DoAll(filename='14266_NGC6946BH1.st')
    tmp = time.time()-tic
    print('Completed in %.3f seconds\n' % tmp)
