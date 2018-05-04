#! /usr/bin/env python
'''
Usage:
./phot_cull3.py */phot

Betterment:
- 'save_best' should just write out, but the 'best' params <<<<< To Do: currently, two separate functions doing this 
- Each filter set gets it's own process, then tasks within <<<<< To Do: map/submit via generators & then combine/reduce results
  are multi-threaded, followed by comparison to find what's 
  optimal via dimensionality reduction
  Note: 'lambda' functions are good for single process. For 
         multi-, need to create a 'generator'

Next:
- Use 1...n quality params in all possible orders   <<<<<  To Do: ready to sandbox this
- Use 1...n filters in all possible order           <<<<<  To Do: nfilters now fixed at two
- Use multiple Dolphot output files to evaluate     <<<<<  To Do: other photometry tools & then qual params
  efficacy of photometry param choice               <<<<<  To Do: not just recovery of input sources but
- Evaluate how good the recommended cuts/phot                     recovery of science assumptions (stellar 
  params are by blind analysis of second image-set                age, stream etc.) that was put in used to 
                                                                  generate source list
Goal:
- Recommend observation plan; choice of:
-- Filters
-- Exposure
-- Dither
- Within constrains of total integration+overhead


Components:
- Read in point source list, contaminate /w background+foreground
- Simulate images
- Run Dolphot
- Estimate optimal observation strategy to maximize science
- Evaluate efficacy for a second set (no-training)
- Iterate till stable

'''
import time, argparse, concurrent.futures, matplotlib
matplotlib.use('Agg')
from matplotlib import cm
from matplotlib import pyplot as plt
plt.ioff()
import numpy as np
from astropy.io import ascii
from scipy.spatial import cKDTree
from os import cpu_count


'''Create worker pools'''
io_pool = concurrent.futures.ThreadPoolExecutor(max_workers=5*cpu_count())
cpu_pool = concurrent.futures.ProcessPoolExecutor(max_workers=cpu_count())


'''WFI Filters and AB-Vega zeropoint differences'''
filters   = np.array(['Z087','Y106','J129','H158','F184'])
AB_Vega = np.array([0.487,0.653,0.958,1.287,1.552])



def DoAll(filename='10_10_dol1/phot',filters=filters,AB_Vega=AB_Vega,
          tol=1,niter=10,nsig=10,sigres=0.05,sigcut=3.2,FR=0.4,RR=0.9):
    sigcuts = np.arange(sigcut-sigres*nsig/2,sigcut+sigres*nsig/2,sigres)
    fileroot,filename = get_fileroot(filename)
    
    data = [ascii.read(fileroot+filters[k]+'_stips.txt',format='ipac')\
            for k in range(len(filters))]
    xy = read_ascii_col(fileroot+filename,'xy',2)
    vega_mags,mag_errors = [read_ascii_col(fileroot+filename,\
                            k,len(filters)) for k in ['mags','errs']]
    SNR,Crowd,Round,Sharp= [read_ascii_col(fileroot+filename,\
            k,len(filters)) for k in ['snr','crowd','round','sharp']]

    all_in = lambda a,b: input_sources(data,a,b)
    all_out = lambda a,b: output_sources(xy,vega_mags,mag_errors,\
                                         SNR,Crowd,Round,Sharp,a,b)
    # To make things simpler, dict-zip 'em all and pass as kwargs array
    _in,_out,_i,_j,_tol,_sigs,_nit,_file,_root,_FR,_RR,_best,_plots=\
                            [],[],[],[],[],[],[],[],[],[],[],[],[]
    for i in range(len(filters)-1):
        for j in range(i,len(filters)-1):
            _t1,_t2 = all_in(i,j),all_out(i,j)
            _in.append(_t1); _out.append(_t2); _i.append(i); _j.append(j)
            _tol.append(tol);_sigs.append(sigcuts);_nit.append(niter)
            _file.append(filename);_root.append(fileroot)
            _FR.append(FR);_RR.append(RR);_best.append(True);_plots.append(True)
    _t = cpu_pool.map(clean_all,_in,_out,_i,_j,\
                      _tol,_sigs,_nit,_RR,_FR,_file,_root,_best,_plots,\
                      chunksize=int(np.ceil(len(_i)/cpu_count())))
    
    return 1


def clean_all(all_in,all_out,i=0,j=0,tol=1,sigcuts=[3.0,3.2,3.5],niter=10,
              RR=0.9,FR=0.25,filename='',fileroot='',best=True,plots=False):
    X,Y,typ_in,x,y,m1,m2,K = unpack_inOut(all_in,all_out)

    ''' Order of quality param use '''
    #nms = ['rnd','shr','err','crd','snr']
    nms = ['snr','err','shr','rnd','crd']
    
    myClean = lambda a: clean_up(X,Y,typ_in,x,y,m1,m2,K,nms,\
                    tol=tol,sigcut=a,niter=niter,RR=RR,FR=FR)
    _rr,_fr,_tt = [],[],[]
    for sigcut in sigcuts:
        # map to io_pool
        _r,_f,_t = myClean(sigcut)
        _rr.append(_r); _fr.append(_f); _tt.append(_t)
    _rr,_fr,_tt,_sig = np.array(_rr), np.array(_fr), np.array(_tt), np.array(sigcuts)
   
    if len(_fr[_fr<FR])>0:
        t = _fr<FR
        _rr,_fr,_tt,_sig = _rr[t],_fr[t],_tt[t],_sig[t]
        if len(_rr)>1:
            t = np.argmax(_rr)
            _rr,_fr,_tt, _sig = _rr[t],_fr[t],_tt[t],_sig[t]   
    else:
        t = np.argmin(_fr)
        _rr,_fr,_tt, _sig = _rr[t],_fr[t],_tt[t],_sig[t]
    
    t = _tt==True
    if best:
        save_best(K,nms,t,_sig,_rr,_fr,filters[i],filters[j+1])
        
    if plots:
        tmp, typ_out = match_in_out(tol,X,Y,x[t],y[t],typ_in)
        clean_out = dict(zip(['m1','m2','x','y','typ_out'],[m1[t],m2[t],x[t],y[t],typ_out]))
        '''
        make_plots(clean_out=clean_out,\
                   filt1=filters[i],filt2=filters[j+1],\
                   filename=filename,fileroot=fileroot,\
                   opt=['clean'])
        '''
        # submit to io_pool
        make_plots(all_in=all_in,all_out=all_out,clean_out=clean_out,\
                   filt1=filters[i],filt2=filters[j+1],\
                   AB_Vega1=AB_Vega[i],AB_Vega2=AB_Vega[j+1],\
                   filename=filename,fileroot=fileroot,\
                   opt=['input','output','clean','diff'],\
                   tol=tol)
    return 1
    

def save_best(K,nms,t,wd=3.2,rr=0,fr=1,filt1='',filt2='',wt=2):
    print('\nFilters: {:s} and {:s}'.format(filt1,filt2))
    print('Recovery Rate:\t {:.2f}\nFalse Rate: \t {:.2f}\nCuts:'.format(rr,fr))
    for s in range(len(nms)):
        xt,yt = K[nms[s]][0][t], K[nms[s]][1][t]
        xtm,xts,ytm,yts = xt.mean(), xt.std(), yt.mean(), yt.std()
        xlo,xhi,ylo,yhi = xtm-wd*xts, xtm+wd*xts, ytm-wd*yts, ytm+wd*yts
        if (nms[s]=='err')|(nms[s]=='crd'):
            xlo,ylo = 0,0
        elif nms[s]=='snr':
            xhi,yhi = xhi*wt,yhi*wt
            if xlo<1: xlo=1
            if ylo<1: ylo=1
        print('{:s}\t{:.2f}\t{:.2f}\t{:.2f}\t{:.2f}'.format(nms[s],xlo,xhi,ylo,yhi))
    return 1


'''Iteratively reject outliers until desired false-rate is achieved'''
def clean_up(X,Y,typ_in,x,y,m1,m2,K,nms,tol=1,sigcut=3.5,niter=10,RR=0.9,FR=0.25):
    rr,fr,t = 0, 1, np.repeat(True,len(x))
    for z in range(niter):
        if (fr>FR):
            for s in range(len(nms)):
                tmp, typ_out = match_in_out(tol,X,Y,x[t],y[t],typ_in)
                k0,k1 = K[nms[s]][0][t], K[nms[s]][1][t]
                p0,p1 = k0[typ_out=='point'], k1[typ_out=='point']
                t1 = myCut(k0,k1,p0,p1,sigcut,nms[int(s)])
                t[t] = t1
            tmp, typ_out = match_in_out(tol,X,Y,x[t],y[t],typ_in)
            rr,fr = get_stat(typ_in,typ_out)
        else:
            break
    # Also return the cuts
    return rr,fr,t


'''Sigma outlier culling'''
def myCut(x,y,xt,yt,wd=3.5,nms='',wt=2):
    xtm,xts,ytm,yts = xt.mean(), xt.std(), yt.mean(), yt.std()
    xlo,xhi,ylo,yhi = xtm-wd*xts, xtm+wd*xts, ytm-wd*yts, ytm+wd*yts
    if (nms=='err')|(nms=='crd'):
        return (x<xhi) & (y<yhi)
    elif (nms=='rnd')|(nms=='shr'):
        return (x<xhi)&(x>xlo)&(y<yhi)&(y>ylo)
    elif nms=='snr':
        if xlo<1: xlo=1
        if ylo<1: ylo=1
        return (x<xhi*wt)&(x>xlo)&(y<yhi*wt)&(y>ylo)

'''Match input to recovered and label recovered'''
def match_in_out(tol,X,Y,x,y,typ_in):
    in1 = matchLists(tol,X,Y,x,y)
    in2 = in1!=-1
    in3 = in1[in2]
    in4 = np.arange(len(x))
    in5 = np.setdiff1d(in4,in3)
    typ_out = np.empty(len(x),dtype='<U10')
    typ_out[in3] = typ_in[in2]
    typ_out[in5] = 'other'
    return in1, typ_out


''' Return recovery rate and false rate for stars'''
def get_stat(typ_in,typ_out):
    all_in, all_recov = len(typ_in), len(typ_out)
    stars_in = len(typ_in[typ_in=='point'])
    stars_recov = len(typ_out[typ_out=='point'])
    recovery_rate = (stars_recov / stars_in)
    false_rate = 1 - (stars_recov / all_recov)
    return recovery_rate,false_rate


''' Quick match; returns index of 2nd list coresponding to position in 1st '''
def matchLists(tol,x1,y1,x2,y2):
    d1 = np.empty((x1.size, 2))
    d2 = np.empty((x2.size, 2))
    d1[:,0],d1[:,1] = x1,y1
    d2[:,0],d2[:,1] = x2,y2
    t = cKDTree(d2)
    tmp, in1 = t.query(d1, distance_upper_bound=tol)
    in1[in1==x2.size] = -1
    return in1


'''Pick sources added in both bands as same object types'''
def input_sources(data,i,j):
    m1_in,m2_in,X1,Y1,X2,Y2 = data[i]['vegamag'],data[j+1]['vegamag'],\
                data[i]['x'],data[i]['y'],data[j+1]['x'],data[j+1]['y']
    typ1_in, typ2_in = data[i]['type'], data[j+1]['type']
    in12 = matchLists(0.1,X1,Y1,X2,Y2)
    m1_in,X1,Y1,typ1_in = m1_in[in12!=-1],X1[in12!=-1],Y1[in12!=-1],typ1_in[in12!=-1]
    in12 = in12[in12!=-1]
    m2_in,X2,Y2,typ2_in = m2_in[in12],X2[in12],Y2[in12],typ2_in[in12]
    tt = typ1_in==typ2_in
    m1_in,m2_in,X,Y,typ_in = m1_in[tt],m2_in[tt],X1[tt],Y1[tt],typ1_in[tt]
    return dict(zip(['m1_in','m2_in','X','Y','typ_in'],[m1_in,m2_in,X,Y,typ_in]))


'''Recovered source photometry and quality params'''
def output_sources(xy,mags,errors,SNR,Crowd,Round,Sharp,i,j):
    nms = ['xy','mag','err','snr','crd','rnd','shr']
    K = [[xy[0],xy[1]],[mags[i],mags[j+1]],[errors[i],errors[j+1]],[SNR[i],SNR[j+1]],\
         [Crowd[i],Crowd[j+1]],[Round[i],Round[j+1]],[Sharp[i],Sharp[j+1]]]
    return dict(zip(nms,K))


'''Take quality cuts on recovered'''
def unpack_inOut(all_in,all_out,tol=1,n=5):
    X,Y,typ_in = all_in['X'], all_in['Y'], all_in['typ_in']
    x,y,m1,m2 = all_out['xy'][0], all_out['xy'][1],\
                all_out['mag'][0], all_out['mag'][1]
    err,rnd,shr,crd,snr = all_out['err'],all_out['rnd'],\
            all_out['shr'],all_out['crd'],all_out['snr']
    K = [[rnd[0],rnd[1]],[shr[0],shr[1]],[err[0],err[1]],\
          [crd[0],crd[1]],[snr[0],snr[1]]]
    nms = ['rnd','shr','err','crd','snr']
    return X,Y,typ_in,x,y,m1,m2,dict(zip(nms,K))


'''CMDs and quality param plotting'''
def make_plots(all_in=[],all_out=[],clean_out=[],\
               filt1='',filt2='',AB_Vega1=0,AB_Vega2=0,
               fileroot='',filename='',cmnt='',tol=1,
               opt=['input','output','clean','count','diff']):
    plot_me = lambda a,b,st,ot,ttl,pre,post: \
              plot_cmd(a,b,filt1=filt1,filt2=filt2,\
                stars=st,other=ot,title=ttl,\
                fileroot=fileroot,outfile=\
                '_'.join((pre,'cmd',filt1,filt2,post,cmnt)))
    plot_it = lambda a,b,filt: \
              plot_xy(x=a,y=a-b,\
                ylim1=-1.5,ylim2=0.5,xlim1=24.5,xlim2=28,\
                ylabel='magIn - magOut',xlabel='magOut',\
                title='In-Out Mag Diff {:s}'.format(filt),\
                fileroot=fileroot,\
                outfile='_'.join(('mag','diff',filt,cmnt)))

    if (('clean' in opt)&(len(clean_out)>0)):
        m1,m2,typ_out = clean_out['m1'],clean_out['m2'],clean_out['typ_out']
        stars,other = typ_out=='point',typ_out!='point'
        plot_me(m1,m2,stars,other,'Cleaned CMD','clean','')
        if 'count' in opt:
            count_stars(m1[stars],[28,28.5,29],'Recovered stars')

    if (('input' in opt)&(len(all_in)>0)):
        m1_in,m2_in,typ_in = all_in['m1_in'],all_in['m2_in'],all_in['typ_in']
        stars,other = typ_in=='point',typ_in!='point'
        plot_me(m1_in,m2_in,stars,other,\
                'Input CMD (Vega)','input','Vega')
        plot_me(m1_in+AB_Vega1,m2_in+AB_Vega2,stars,other,\
                'Input CMD (AB)','input','AB')
        if 'count' in opt:
            count_stars((m1_in[stars])+AB_Vega1,[28,28.5,29],'Input stars')

    if (('output' in opt)&(len(all_out)>0)):
        m1,m2 = all_out['mag'][0], all_out['mag'][1]
        if 'count' in opt:
            count_stars(m1,[28,28.5,29],'Recovered sources')
        if 'input' in opt:
            X,Y,x,y = all_in['X'],all_in['Y'], all_out['xy'][0], all_out['xy'][1]
            in1, typ_out = match_in_out(tol,X,Y,x,y,typ_in)
            stars,other = typ_out=='point',typ_out!='point'
            if 'count' in opt:
                count_stars(m1[stars],[28,28.5,29],'Point Sources')
            if (('diff' in opt)|('diff2' in opt)):
                t1 = (in1!=-1)&(typ_in=='point')
                m1in,m2in,m1t,m2t = m1_in[t1],m2_in[t1],m1[in1[t1]],m2[in1[t1]]
                t2 = typ_out[in1[t1]]=='point'
                m1in,m2in,m1t,m2t=m1in[t2],m2in[t2],m1t[t2],m2t[t2]
                if 'diff' in opt:
                    plot_it(m1in,m1t,filt1)
                if 'diff2' in opt:
                    plot_it(m2in,m2t,filt2)
        else:
            typ_out = np.repeat('other',len(m1))
        stars,other = typ_out=='point',typ_out!='point'
        plot_me(m1,m2,stars,other,'Full CMD','full','')
    return 0


'''Make CMD'''
def plot_cmd(m1,m2,e1=[],e2=[],filt1='',filt2='',stars=[],other=[],\
              fileroot='',outfile='test',fmt='png',\
              xlim1=-1.5,xlim2=3.5,ylim1=29.5,ylim2=20.5,n=4,title=''):
    m1m2 = m1-m2
    plt.rc("font", family='serif', weight='bold')
    plt.rc("xtick", labelsize=15); plt.rc("ytick", labelsize=15)
    fig = plt.figure(1, ((10,10)))
    fig.suptitle(title,fontsize=5*n)
    if len(stars[stars])==0:
        m1m2t,m2t = plotHess(m1m2,m2)
        plt.plot(m1m2t,m2t,'k.',markersize=2,alpha=0.75,zorder=3)
    else:
        plt.plot(m1m2[stars],m2[stars],'b.',markersize=2,\
            alpha=0.75,zorder=2,label='Stars: %d' % len(m2[stars]))
        plt.plot(m1m2[other],m2[other],'k.',markersize=1,\
            alpha=0.5,zorder=1,label='Other: %d' % len(m2[other]))
        plt.legend(loc=4,fontsize=20)
    if (len(e1)&len(e2)):
        m1m2err = np.sqrt(e1**2+e2**2)
        plot_error_bars(m2,e2,m1m2err,xlim1,xlim2,ylim1,slope=[])
    plt.xlim(xlim1,xlim2); plt.ylim(ylim1,ylim2)
    plt.xlabel(str(filt1+'-'+filt2),fontsize=20)
    plt.ylabel(filt2,fontsize=20)
    print('Writing out:\n',fileroot+outfile+'.'+str(fmt))
    plt.savefig(fileroot+outfile+'.'+str(fmt))
    return plt.close()


'''Simple Plotting'''
def plot_xy(x,y,xlabel='',ylabel='',title='',stars=[],other=[],\
              xlim1=-1,xlim2=1,ylim1=-7.5,ylim2=7.5,\
              fileroot='',outfile='test',fmt='png',n=4):
    plt.rc("font", family='serif', weight='bold')
    plt.rc("xtick", labelsize=15); plt.rc("ytick", labelsize=15)
    fig = plt.figure(1, ((10,10)))
    fig.suptitle(title,fontsize=5*n)
    if not len(x[other]):
        plt.plot(x, y,'k.',markersize=1,alpha=0.5)
    else:
        plt.plot(x[stars],y[stars],'b.',markersize=2,\
            alpha=0.5,zorder=2,label='Stars: %d' % len(x[stars]))
        plt.plot(x[other],y[other],'k.',markersize=1,\
            alpha=0.75,zorder=1,label='Other: %d' % len(x[other]))
        plt.legend(loc=4,fontsize=20)
    plt.xlim(xlim1,xlim2); plt.ylim(ylim1,ylim2)
    plt.xlabel(xlabel,fontsize=20)
    plt.ylabel(ylabel,fontsize=20)
    plt.savefig(fileroot+outfile+'.'+str(fmt))
    return plt.close()


'''Overplot hess diagram for densest regions'''
def plotHess(color,mag,binsize=0.1,threshold=25):
    if not len(color)>threshold:
        return color,mag
    mmin,mmax = np.amin(mag),np.amax(mag)
    cmin,cmax = np.amin(color),np.amax(color)
    nmbins = np.ceil((cmax-cmin)/binsize)
    ncbins = np.ceil((cmax-cmin)/binsize)
    Z, xedges, yedges = np.histogram2d(color,mag,\
                            bins=(ncbins,nmbins))
    X = 0.5*(xedges[:-1] + xedges[1:])
    Y = 0.5*(yedges[:-1] + yedges[1:])
    y, x = np.meshgrid(Y, X)
    z = np.ma.array(Z, mask=(Z==0))
    levels = np.logspace(np.log10(threshold),\
            np.log10(np.amax(z)),(nmbins/ncbins)*20)
    if (np.amax(z)>threshold)&(len(levels)>1):
        cntr=plt.contourf(x,y,z,cmap=cm.jet,levels=levels,zorder=0)
        cntr.cmap.set_under(alpha=0)
        x,y,z = x.flatten(),y.flatten(),Z.flatten()
        x = x[z>2.5*threshold]
        y = y[z>2.5*threshold]
        mask = np.zeros_like(mag)
        for col,m in zip(x,y):
            mask[(m-binsize<mag)&(m+binsize>mag)&\
                 (col-binsize<color)&(col+binsize>color)]=1
            mag = np.ma.array(mag,mask=mask)
            color = np.ma.array(color,mask=mask)
    return color,mag


'''Split filepath and filename'''
def get_fileroot(filename):
    if '/' in filename:
        tmp = filename.split('/')[-1]
        fileroot = filename[:-len(tmp)]
        filename = tmp
    else:
        fileroot = ''
    return fileroot, filename


'''Return requested columns in a NumPy array'''
def read_ascii_col(filename,suff,ncol):
    if isinstance(ncol,int):
        ncol = range(ncol)
    _tmp, _file = [], '.'.join((filename,suff))
    _data = ascii.read(_file)
    for i in ncol:
        _tmp.append(_data['col'+str(i+1)])
    return np.array(_tmp)


'''How many stars brighter than threshold'''
def count_stars(m1,cut,cmnt):
    for k in cut:
        print(' '.join((cmnt,'brighter than {:.2f}:\t{:d}'.format(k,len(m1[m1<k])))))
    return



'''Argument parser template'''
def parse_all():
    parser = argparse.ArgumentParser()
    parser.add_argument('filenames', nargs='+',help='Photomtery file names')
    parser.add_argument('--NITER', '-niter', type=int, dest='niter', default=10, help='Maximum iterations')
    parser.add_argument('--NSIG', '-nsig', type=int, dest='nsig', default=20, help='# of other sigma thresholds to try')
    parser.add_argument('--SIGRES', '-sigres', type=float, dest='sigres', default=0.05, help='Sigma stepsize')
    parser.add_argument('--SIGMA', '-sig', type=float, dest='sig', default=3.2, help='Sigma threshold removing outliers')
    parser.add_argument('--RADIUS', '-tol', type=float, dest='tol', default=1, help='Matching radius in pixels')
    parser.add_argument('--FALSE', '-fr', type=float, dest='fr', default=0.4, help='Desired False Rate')
    parser.add_argument('--RECOV', '-rr', type=float, dest='rr', default=0.5, help='Desired Recovery Rate')
    return parser.parse_args()


'''If executed from command line'''
if __name__ == '__main__':
    tic = time.time()
    assert 3/2 == 1.5, 'Not running Python3 may lead to wrong results'
    args = parse_all()
    _do = lambda x: DoAll(x, tol=args.tol,\
                          niter=args.niter, nsig=args.nsig,\
                          sigres=args.sigres, sigcut=args.sig,\
                          FR=args.fr,RR=args.rr)
    for filename in args.filenames:
        _do(filename)
    else:
        io_pool.shutdown(wait=True)
        cpu_pool.shutdown(wait=True)
        print('\n\nCompleted in %.3f seconds \n' % (time.time()-tic))
