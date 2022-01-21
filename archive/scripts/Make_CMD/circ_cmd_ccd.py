#!/usr/bin/env python
'''
info here
'''
from __future__ import (absolute_import, division, print_function, unicode_literals)
import matplotlib
matplotlib.use('Agg')
from matplotlib import cm
from matplotlib import pyplot as plt
plt.ioff()

import numpy as np
from astropy.io import fits
from astropy.io import ascii
from astropy.table import Table

grid=3
filename = '14266_NGC6946BH1.gst.fits'
filt = ['F438W','F606W','F814W']

extinct = [1.241,0.938,0.515]

iso_dir = 'age_dat1/'
iso_file = ['age07.5.dat','age10.dat','age14.dat','age19.dat','age25.dat']

isocol = ['r-','g-','b-','m-','c-']

dist=6.8
mu = 25+5*np.log10(dist)
pctol=50
pixscale=0.03886
xref=2278.8
yref=2047.8

fileroot = 'Vanisher_gst'

def DoAll(n=grid):
    arctol = pctol / (dist*1e6/206265)
    pixtol = arctol / pixscale
    X,Y,RA,DEC,vega_mags,mag_errors,detector = read_phot_fits_table(filename)
    mags, errs = np.array(vega_mags).T, np.array(mag_errors).T

    plt.rc('font', family='serif', weight='bold')
    plot_1ccd(X,Y,mags,0,1,2,(pixtol/np.sqrt(1.25e-3)),fileroot,n)

    ages = [ascii.read(iso_dir+iso_file[i]) for i in range(len(iso_file))]
    ages = np.array([[ages[i][filt[0]+'mag']+mu+extinct[0],\
                      ages[i][filt[1]+'mag']+mu+extinct[1],\
                      ages[i][filt[2]+'mag']+mu+extinct[2]] \
                      for i in range(len(iso_file))])

    plot_1cmd(X,Y,mags,ages,0,1,(pixtol/np.sqrt(1.25e-3)),fileroot,n)
    plot_1cmd(X,Y,mags,ages,1,2,(pixtol/np.sqrt(1.25e-3)),fileroot,n)
    
    in1 = np.arange(len(X))
    in1 = in1[(np.fabs(X-xref)<pixtol*(n//2+1))&(np.fabs(Y-yref)<pixtol*(n//2+1))]
    X,Y,mags,errs = X[in1],Y[in1],mags[in1,:],errs[in1,:]
    
    plot_circ_cmds(X,Y,mags,ages,0,1,pixtol,fileroot,n)
    plot_circ_cmds(X,Y,mags,ages,1,2,pixtol,fileroot,n)
    plot_circ_ccds(X,Y,mags,0,1,2,pixtol,fileroot,n)
    return None

def plot_circ_cmds(x,y,mags,ages,id1,id2,pixtol,outroot,n):
    pixtol2 = pixtol**2
    filt1,filt2,m1m2,m2 = filt[id1],filt[id2],mags[:,id1]-mags[:,id2],mags[:,id2]
    x0,x1 = xref-(n//2)*(pixtol*np.sqrt(2)),xref+(n//2)*(pixtol*np.sqrt(2))
    y0,y1 = yref-(n//2)*(pixtol*np.sqrt(2)),yref+(n//2)*(pixtol*np.sqrt(2))
    xcen,ycen = np.linspace(x0,x1,n),np.linspace(y0,y1,n)
    plt.rc("xtick", labelsize=3*n); plt.rc("ytick", labelsize=3*n)
    fig = plt.figure(1,((n*5,n*5)))
    fig.suptitle(' '.join([outroot,filt1,filt2]),fontsize=7*n)
    for i in range(n):
        for j in range(n):
            xo,yo = xcen[i],ycen[n-j-1]
            in1 = np.arange(len(x))
            in1 = in1[((x-xo)**2 + (y-yo)**2)<pixtol2]
            ax = plt.subplot2grid((n,n),(i, (n-j-1)))
            ax.set_xlim(-0.6,3.7), ax.set_ylim(28.05,19.5)
            ax.plot(m1m2[in1],m2[in1],'kx',markersize=n*2,zorder=1)
            for k in range(ages.shape[0]):
                m1m2t,m2t = ages[k][id1]-ages[k][id2],ages[k][id2]
                ax.plot(m1m2t,m2t,isocol[k],zorder=3)
            
            if ((i==(n//2))&(j==(n//2))&(id1==1)&(id2==2)):
                ax.plot(2.32,20.77,'ko',markersize=n*3,zorder=2)
            elif ((i==(n//2))&(j==(n//2))&(id1==0)&(id2==1)):
                ax.plot(3,23.09,'ko',markersize=n*3,zorder=2)
            if ax.is_first_col():
                ax.set_ylabel(filt2,fontsize=5*n)
            else:
                ax.set_yticklabels([])
            if ax.is_last_row():
                ax.set_xlabel('-'.join([filt1,filt2]),fontsize=5*n)
            else:
                ax.set_xticklabels([])
    fig.subplots_adjust(wspace=0,hspace=0)
    print('Writing out:\n', '_'.join([outroot,str(id1),str(id2),'CIRCcmd.png']))
    plt.savefig('_'.join([outroot,str(id1),str(id2),'CIRCcmd.png']))
    return plt.close('all')

def plot_1cmd(x,y,mags,ages,id1,id2,pixtol,outroot,n):
    pixtol2 = pixtol**2
    filt1,filt2,m1m2,m2 = filt[id1],filt[id2],mags[:,id1]-mags[:,id2],mags[:,id2]
    in1 = np.arange(len(x))
    in1 = in1[((x-xref)**2 + (y-yref)**2)<pixtol2]
    plt.rc("xtick", labelsize=15); plt.rc("ytick", labelsize=15)
    fig = plt.figure(1, ((10,10)))
    fig.suptitle(' '.join([outroot,filt1,filt2]),fontsize=20)
    plt.xlim(-0.6,3.7), plt.ylim(28.05,19.5)
    plt.plot(m1m2[in1],m2[in1],'k.',markersize=2,zorder=1)
    if ((id1==1)&(id2==2)):
        plt.plot(2.32,20.77,'ko',markersize=10,zorder=2)
    elif ((id1==0)&(id2==1)):
        plt.plot(3,23.09,'ko',markersize=10,zorder=2)
    for k in range(ages.shape[0]):
        m1m2t,m2t = ages[k][id1]-ages[k][id2],ages[k][id2]
        plt.plot(m1m2t,m2t,isocol[k],zorder=3)
    plt.xlabel('-'.join([filt1,filt2]),fontsize=20)
    plt.ylabel(filt2,fontsize=20)
    print('Writing out: ','_'.join([outroot,str(id1),str(id2),'bigCIRCcmd.png']))
    plt.savefig('_'.join([outroot,str(id1),str(id2),'bigCIRCcmd.png']))
    return plt.close('all')

def plot_circ_ccds(x,y,mags,id1,id2,id3,pixtol,outroot,n):
    pixtol2 = pixtol**2
    filt1,filt2,filt3,m1,m2,m3 = filt[id1],filt[id2],filt[id3],mags[:,id1],mags[:,id2],mags[:,id3]
    m1m2,m2m3 = m1-m2,m2-m3
    x0,x1 = xref-(n//2)*(pixtol*np.sqrt(2)),xref+(n//2)*(pixtol*np.sqrt(2))
    y0,y1 = yref-(n//2)*(pixtol*np.sqrt(2)),yref+(n//2)*(pixtol*np.sqrt(2))
    xcen,ycen = np.linspace(x0,x1,n),np.linspace(y0,y1,n)
    print('X centers = ',xcen)
    print('Y Centers = ',ycen)
    print('50pc in Pixels = ',pixtol)
    plt.rc("xtick", labelsize=3*n); plt.rc("ytick", labelsize=3*n)
    fig = plt.figure(1,((n*5,n*5)))
    fig.suptitle('     '.join([outroot,filt1,filt2,filt3]),fontsize=7*n)
    for i in range(n):
        for j in range(n):
            xo,yo = xcen[i],ycen[n-j-1]
            in1 = np.arange(len(x))
            in1 = in1[((x-xo)**2 + (y-yo)**2)<pixtol2]
            ax = plt.subplot2grid((n,n),(i, (n-j-1)))
            ax.set_xlim(-0.6,3.7), ax.set_ylim(-0.6,3.7)
            ax.plot(m1m2[in1],m2m3[in1],'kx',markersize=n*2,zorder=1)
            if ((i==(n//2))&(j==(n//2))):
                ax.plot(2.32,3,'ko',markersize=n*3,zorder=2)
            write_input_file(m1[in1],m2[in1],m3[in1],i,n-j-1,fileroot,n)
            write_coord_file(x[in1],y[in1],m1[in1],m2[in1],m3[in1],i,n-j-1,fileroot,n)
            if ax.is_first_col():
                ax.set_ylabel('-'.join([filt2,filt3]),fontsize=5*n)
            else:
                ax.set_yticklabels([])
            if ax.is_last_row():
                ax.set_xlabel('-'.join([filt1,filt2]),fontsize=5*n)
            else:
                ax.set_xticklabels([])
    fig.subplots_adjust(wspace=0,hspace=0)
    print('Writing out:\n', '_'.join([outroot,'CIRCccd.png']))
    plt.savefig('_'.join([outroot,'CIRCccd.png']))
    return plt.close('all')
    
def plot_1ccd(x,y,mags,id1,id2,id3,pixtol,outroot,n):
    pixtol2 = pixtol**2
    filt1,filt2,filt3,m1,m2,m3 = filt[id1],filt[id2],filt[id3],mags[:,id1],mags[:,id2],mags[:,id3]
    m1m2,m2m3 = m1-m2,m2-m3
    in1 = np.arange(len(x))
    in1 = in1[((x-xref)**2 + (y-yref)**2)<pixtol2]
    write_input_file(m1[in1],m2[in1],m3[in1],n,n,fileroot,n)
    write_coord_file(x[in1],y[in1],m1[in1],m2[in1],m3[in1],n,n,fileroot,n)    
    plt.rc("xtick", labelsize=15); plt.rc("ytick", labelsize=15)
    fig = plt.figure(1, ((10,10)))
    fig.suptitle('     '.join([outroot,filt1,filt2,filt3]),fontsize=20)
    plt.xlim(-0.6,3.7); plt.ylim(-0.6,3.7)
    plt.plot(m1m2[in1],m2m3[in1],'k.',markersize=2,zorder=1)
    plt.plot(3,2.32,'ko',markersize=10,zorder=2)
    plt.xlabel('-'.join([filt1,filt2]),fontsize=20)
    plt.ylabel('-'.join([filt2,filt3]),fontsize=20)
    print('Writing out: ','_'.join([outroot,'bigCIRCccd.png']))
    plt.savefig('_'.join([outroot,'bigCIRCccd.png']))
    return plt.close('all')

def write_input_file(m1,m2,m3,i,j,fileroot,n):
    outfile = fileroot+''.join(['_',str(n),str(i),str(j),'.txt'])
    in1 = [i for i in range(len(m1)) if ((m1[i]<30)&(m2[i]<30)|(m2[i]<30)&(m3[i]<30))]
    tab = [m1[in1],m2[in1],m3[in1]]
    nms = ('m1','m2','m3')
    fmt = {'m1':'%8.3f', 'm2':'%8.3f', 'm3':'%8.3f'}
    t = Table(tab, names=nms)
    ascii.write(t, outfile, format='fixed_width', delimiter='', formats=fmt)
    return print('Wrote out: ', outfile)


def write_coord_file(x,y,m1,m2,m3,i,j,fileroot,n):
    outfile = fileroot+''.join(['_',str(n),str(i),str(j),'_xy.txt'])
    in1 = [i for i in range(len(m1)) if ((m1[i]<30)&(m2[i]<30)|(m2[i]<30)&(m3[i]<30))]
    tab = [x[in1],y[in1],m1[in1],m2[in1],m3[in1]]
    nms = ('x','y','m1','m2','m3')
    fmt = {'x':'%8.3f', 'y':'%8.3f', 'm1':'%8.3f', 'm2':'%8.3f', 'm3':'%8.3f'}
    t = Table(tab, names=nms)
    ascii.write(t, outfile, format='fixed_width', delimiter='', formats=fmt)
    return print('Wrote out: ', outfile)


def read_phot_fits_table(filename):
    print('Reading in: ', filename)
    photTable = fits.open(filename)
    detector = photTable[0].header['CAMERA']
    data = photTable[1].data; del photTable
    vega_mags  = [data[filt[0]+'_VEGA'], data[filt[1]+'_VEGA'], data[filt[2]+'_VEGA']]
    mag_errors = [data[filt[0]+'_ERR'], data[filt[1]+'_ERR'], data[filt[2]+'_ERR']]
    X,Y,RA,DEC = data['X'],data['Y'],data['RA'],data['DEC']
    return X,Y,RA,DEC,vega_mags,mag_errors,detector

if __name__ == '__main__':
    import sys, time
    tic = time.time()
    print('\n\nPython check: This should be 1.500 = %.3f \n' % (3/2))
    DoAll()
    print('\n\nCompleted in %.3f seconds \n' % (time.time()-tic))
