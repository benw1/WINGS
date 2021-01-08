#! /usr/bin/env python
'''
Read in photometry fits table, make CMDs. Usage:
./make_cmd.py filename
Examples:
./make_cmd.py 14266_NGC6946BH1.gst.fits
ls $PATH/*.fits | xargs ./make_cmd.py
./make_cmd.py 14266_NGC6946BH1.gst.fits -x1 -1 -x2 5 -y1 27 -y2 12 -bin 0.1 -lvl [100,150,200,250,300,400,600,800,1200,1600,2000] -thr 150 -pid 10000 -grid 5 -targ Vanisher

'''
from __future__ import (absolute_import, division, print_function, unicode_literals)

import matplotlib
matplotlib.use('Agg')
from matplotlib import cm
from matplotlib import pyplot as plt
plt.ioff()

from wpipe import *
from astropy.io import fits
from astropy.io import ascii
import time, argparse, os, subprocess

def register(PID,task_name):
   myPipe = Store().select('pipelines').loc[int(PID)]
   myTask = Task(task_name,myPipe).create()
   _t = Task.add_mask(myTask,'*','start','*')
   _t = Task.add_mask(myTask,'test_wpipe.py','test','*')
   return


def makeCMD(filenames,xlim1=[],xlim2=[],ylim1=[],ylim2=[],binsize=[],levels=[],threshold=[],pid=[],target=[],n=4,fmt='png'):
    for filename in filenames:
        print('\nReading in:\n', filename)
        X,Y,filters,vega_mags,mag_errors = read_phot_fits_table(filename)
        print('Got filters:\n', filters)

        if '/' in filename:
            tmp = filename.split('/')[-1]
            fileroot = filename[:-len(tmp)]
            filename = tmp
        else:
            fileroot = ''
            
        for i in range(len(filters)-1):
           for j in range(i,len(filters)-1):
               outfile = filename.split('.')[0]+'_'+filters[i]+'_'+filters[j+1]+'.'+filename.split('.')[1]
               plot_cmds(X,Y,vega_mags[i],vega_mags[j+1],mag_errors[i],mag_errors[j+1],filters[i],filters[j+1],\
                         fileroot,outfile,fmt,xlim1,xlim2,ylim1,ylim2,binsize,levels,threshold,pid,target,n)
               outfile = []
    return None


def plot_cmds(XPIX,YPIX,m1,m2,err1,err2,filt1,filt2,fileroot,outfile,fmt,
              xlim1,xlim2,ylim1,ylim2,binsize,levels,threshold,pid,target,n):
    XPIX,YPIX,m2,m1m2,m2err,m1m2err,xlim1,xlim2,ylim1,ylim2,slope = \
        set_limits(filt1,filt2,m1,m2,err1,err2,XPIX,YPIX,xlim1,xlim2,ylim1,ylim2)
    plt.rc("font", family='serif', weight='bold')
    plot_1cmd(m2,m1m2,m2err,m1m2err,filt1,filt2,fileroot,outfile,fmt,xlim1,xlim2,ylim1,ylim2,slope,binsize,levels,threshold,pid,target)
    plot_gridcmd(XPIX,YPIX,m2,m1m2,m2err,m1m2err,filt1,filt2,fileroot,outfile,fmt,xlim1,xlim2,ylim1,ylim2,slope,binsize,levels,threshold,pid,target,n)
    plt.close('all')
    return None


def plot_1cmd(m2,m1m2,m2err,m1m2err,filt1,filt2,fileroot,outfile,fmt,
              xlim1,xlim2,ylim1,ylim2,slope,binsize,levels,threshold,pid,target):
    plt.rc("xtick", labelsize=15); plt.rc("ytick", labelsize=15)
    fig = plt.figure(1, ((10,10)))
    fig.suptitle(get_title(outfile,pid,target,len(m2)),fontsize=20)
    m1m2t,m2t = plotHess(m1m2,m2,binsize,levels,threshold,n=1)
    plt.plot(m1m2t,m2t,'k.',markersize=2,zorder=1)
    plot_error_bars(m2,m2err,m1m2err,xlim1,xlim2,ylim1,slope)
    plt.xlim(xlim1,xlim2); plt.ylim(ylim1,ylim2)
    plt.xlabel(str(filt1+'-'+filt2),fontsize=20)
    plt.ylabel(filt2,fontsize=20)
    print('Writing out:\n',fileroot+outfile+'_cmd.'+str(fmt))
    plt.savefig(fileroot+outfile+'_cmd.'+str(fmt))
    return None


def plot_gridcmd(XPIX,YPIX,m2,m1m2,m2err,m1m2err,filt1,filt2,fileroot,outfile,fmt,
                 xlim1,xlim2,ylim1,ylim2,slope,binsize,levels,threshold,pid,target,n):
    plt.rc("xtick", labelsize=3*n); plt.rc("ytick", labelsize=3*n)
    fig = plt.figure(2,((n*5,n*5)))
    fig.suptitle(get_title(outfile,pid,target,len(m2)),fontsize=10*n)
    xpixmin,xpixmax,ypixmin,ypixmax = np.amin(XPIX),np.amax(XPIX),np.amin(YPIX),np.amax(YPIX)
    for i in range(n):
        for j in range(n):
            xlo,ylo = xpixmin+j*(xpixmax/n),ypixmin+i*(ypixmax/n)
            xhi,yhi = xlo+((xpixmax-xpixmin)/n),ylo+((ypixmax-ypixmin)/n)
            in1 = trim_index(XPIX,xlo,xhi,YPIX,ylo,yhi)
            ax = plt.subplot2grid((n,n),(i, (n-j-1)))
            ax.set_xlim(xlim1,xlim2), ax.set_ylim(ylim1,ylim2)
            if in1:
                mag,color,merr,cerr = m2[in1],m1m2[in1],m2err[in1],m1m2err[in1]
                colort,magt=plotHess(color,mag,binsize,levels,threshold,n)
                ax.plot(colort,magt,'k.',markersize=n,zorder=1)
                plot_error_bars(mag,merr,cerr,xlim1,xlim2,ylim1,slope)
            if ax.is_first_col():
                ax.set_ylabel(filt2,fontsize=5*n)
            else:
                ax.set_yticklabels([])
            if ax.is_last_row():
                ax.set_xlabel(str(filt1+'-'+filt2),fontsize=5*n)
            else:
                ax.set_xticklabels([])
    fig.subplots_adjust(wspace=0,hspace=0)
    print('Writing out:\n',fileroot+outfile+'_gridcmd.'+str(fmt))
    plt.savefig(fileroot+outfile+'_gridcmd.'+str(fmt))
    return None


def plotHess(color,mag,binsize,levels,threshold,n):
    if not binsize:
        binsize=0.1
    if not threshold:
        threshold = 100/n
    if not len(color)>threshold:
        return color,mag
    mmin,mmax,cmin,cmax=np.amin(mag),np.amax(mag),np.amin(color),np.amax(color)
    nmbins,ncbins = np.ceil((mmax-mmin))/binsize,np.ceil((cmax-cmin)/binsize)
    Z, xedges, yedges = np.histogram2d(color,mag,bins=(ncbins,nmbins))
    X = 0.5*(xedges[:-1] + xedges[1:])
    Y = 0.5*(yedges[:-1] + yedges[1:])
    y, x = np.meshgrid(Y, X)
    z = np.ma.array(Z, mask=(Z==0))
    if not levels:
        levels = np.logspace(np.log10(threshold),np.log10(np.amax(z)),(nmbins/ncbins)*20)
    else:
        levels = levels[1:-1].split(',')
        levels = np.array([float(levels[i])for i in range(len(levels))])
        if threshold<levels[0]:
            levels[0]=threshold
        if np.amax(z)>levels[-1]:
            levels[-1]=np.amax(z)
        else:
            levels = levels[levels<np.amax(z)]
    if (np.amax(z)>threshold)&(len(levels)>1):
        cntr=plt.contourf(x,y,z,cmap=cm.jet,levels=levels,zorder=2)
        cntr.cmap.set_under(alpha=0)
        x,y,z = x.flatten(),y.flatten(),Z.flatten()
        x = x[z>2.5*threshold]
        y = y[z>2.5*threshold]
        mask = np.zeros_like(mag)
        for col,m in zip(x,y):
            mask[(m-binsize<mag)&(m+binsize>mag)&(col-binsize<color)&(col+binsize>color)]=1
            mag = np.ma.array(mag,mask=mask)
            color = np.ma.array(color,mask=mask)
    return color,mag


def plot_error_bars(mag,merr,cerr,xlim1,xlim2,ylim1,slope):
    mbins = np.arange(np.amax(mag),np.amin(mag),-0.5)
    for m in mbins:
        in1 = trim_index(mag,m-0.5,m+0.5)
        if len(in1)>100:
            mavg = np.mean(merr[in1])
            cavg = np.mean(cerr[in1])
            plt.errorbar(xlim1+0.75,m,xerr=cavg,yerr=mavg,ecolor='r',elinewidth=2,capsize=0)
    if slope:
        plt.arrow(xlim2-1.8, ylim1-1.7, 0.3, 0.3*slope, width=0.05, head_width=0.3, head_length=0.1, facecolor='r', edgecolor='r')
    return None



def read_phot_fits_table(filename):
    photTable = fits.open(filename)
    filternames = sort_filters(photTable[0].header['FILTERS'].split(','))
    detector = photTable[0].header['CAMERA']
    data = photTable[1].data; del photTable
    vega_mags  = [data[filt+'_VEGA'] for filt in filternames]
    mag_errors = [data[filt+'_ERR'] for filt in filternames]
    X,Y,RA,DEC = data['X'],data['Y'],data['RA'],data['DEC']
    return X,Y,filternames,vega_mags,mag_errors


def sort_filters(filternames):
    filters = np.array([filt for filt in filternames if filt])
    tmp = np.array([int(filters[i][1:4]) for i in range(len(filters))])
    return np.concatenate((np.sort(filters[tmp>200]),np.sort(filters[tmp<200])))


def set_limits(filt1,filt2,m1,m2,err1,err2,XPIX,YPIX,xlim1,xlim2,ylim1,ylim2):
    xlim1,slope=-1.5,[]
    if not xlim2:
        xlim2=5.5
    if not ylim1:
        ylim1=29
    if not ylim2:
        ylim2=15
        
    if '160' in filt2:
        if '110' in filt1:
            xlim1,xlim2 = -1.5,2.5
        elif '475' in filt1:
            xlim1,xlim2 = -1.5,9.5
        elif ('275' in filt1) or ('336') in filt1:
            xlim1,xlim2 = -3,6.5
        elif '814' in filt1:
            xlim1,xlim2 = -1.5,4.5
        elif '110' in filt1:
            slope = 0
    elif '110' in filt2:
        if '475' in filt1:
            xlim1,xlim2 = -1.5,7.5
        elif ('275' in filt1) or ('336' in filt1):
            xlim1,xlim2 = -3,6.5
        elif '814' in filt1:
            xlim1,xlim2 = -1.5,4.5
    elif '814' in filt2:
        if '606' in filt1:
            slope = 1.8
        elif '555' in filt1:
            slope = 1.4
        elif '475' in filt1:
            xlim1,xlim2 = -1.5,7
            slope=1
        elif '435' in filt1:
            xlim1,xlim2 = -1.5,6
            slope = 0.85
        elif '336' in filt1:
            xlim1,xlim2 = -2.5,8
        elif '275' in filt1:
            xlim1,xlim2 = -3,8
        elif xlim2<4:
            xlim2=4
    elif ('435' in filt2) or ('438' in filt2) or ('475' in filt2):
        xlim1,xlim2 = -3,4
    elif '606' in filt2:
        if '475' in filt1:
            slope=3.5
        elif '435' in filt1:
            slope=2.4
    elif (('555' in filt2)&('435' in filt1)):
        slope = 3.7
    elif '336' in filt2:
        xlim1,xlim2 = -1.5,3.5
        
    m1m2, m1m2err = m1-m2, np.sqrt(err1**2+err2**2)
    in1 = trim_index(m1m2,xlim1,xlim2,m2,ylim2,ylim1)
    if (np.amax(m2[in1])+0.5)<ylim1:
        ylim1 = np.amax(m2[in1])+0.5
    if (np.amin(m2[in1])-0.5)>ylim2:
        ylim2 = np.amin(m2[in1])-0.5
    in1 = trim_index(m1m2,xlim1,xlim2,m2,ylim2,ylim1)
    return XPIX[in1],YPIX[in1],m2[in1],m1m2[in1],err2[in1],m1m2err[in1],\
        xlim1,xlim2,ylim1,ylim2,slope


def get_title(outfile,pid,target,num):
    if not pid:
        pid = outfile.split('_')[0]
    if not target:
        target = outfile.split('_')[1]
    if '.gst' in outfile:
        good = 'GST'
    elif '.st' in outfile:
        good = 'GST'
    else:
        good = ''
    return 'PID: '+pid+'    Target: '+target+'    '+good+' Count: '+str(num)


def trim_index(x,xlo,xhi,y=[],ylo=[],yhi=[]):
    i = np.arange(len(x))
    if len(y)>0:
        return i[(x>xlo)&(x<xhi)&(y>ylo)&(y<yhi)]
    else:
        return i[(x>xlo)&(x<xhi)]

    
def parse_all():
    parser = argparse.ArgumentParser()
    #parser.add_argument('filenames', nargs='+',help='Photomtery fits tables list')
    parser.add_argument('--FORMAT',   '-fmt', dest='fmt',  default='png',
                        help='Format of saved figures')
    parser.add_argument('--XMIN', '-x1', type=float, dest='xlim1', default=[],
                        help='Color axis blue limit')
    parser.add_argument('--XMAX', '-x2', type=float, dest='xlim2', default=[],
                        help='Color axis red limit')
    parser.add_argument('--YMIN', '-y1', type=float, dest='ylim1', default=[],
                        help='Mag axis faint limit')
    parser.add_argument('--YMAX', '-y2', type=float, dest='ylim2', default=[],
                        help='Mag axis bright limit')
    parser.add_argument('--BINSIZE',   '-bin', type=float, dest='binsize', default=[],
                        help='Histogram bin size')
    parser.add_argument('--LEVELS',    '-lvl', dest='levels', default=[],
                        help='Contour levels')
    parser.add_argument('--THRESHOLD', '-thr', type=float, dest='threshold', default=[],
                        help='Threshold for triggering countours')
    parser.add_argument('--PID', '-pid', dest='pid', default=[],
                        help='Process ID')
    parser.add_argument('--GRID', '-grid', type=int, dest='grid', default=4,
                        help='Spatial grid per axis')
    parser.add_argument('--TARGET', '-targ', dest='target', default=[],
                        help='Name of Target')
    parser.add_argument('--R','-R', dest='REG', action='store_true',
                        help='Specify to Register')
    parser.add_argument('--P','-p',type=int,  dest='PID',
                        help='Pipeline ID')
    parser.add_argument('--N','-n',type=str,  dest='task_name',
                        help='Name of Task to be Registered')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_all()
    if args.REG:
       _t = register(args.PID,args.task_name)
    else:
        makeCMD(filenames=args.filenames,xlim1=args.xlim1,xlim2=args.xlim2,ylim1=args.ylim1,ylim2=args.ylim2,binsize=args.binsize,\
              levels=args.levels,threshold=args.threshold,pid=args.pid,target=args.target,n=args.grid,fmt=args.fmt)
