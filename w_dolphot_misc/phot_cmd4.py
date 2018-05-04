#! /usr/bin/env python3
'''
First use these awk commands to split raw DOLPHOT output
file named (e.g.) 'phot' for desired column numbers in
(e.g.) phot.columns:
 
awk '{printf "%.2f %.2f\n", $3, $4}' phot > phot.xy
xy2sky -d -n 10 ../H158.fits @phot.xy | awk '{printf "%.9f %.9f\n", $1, $2}' > phot.radec
awk '{printf "%.3f %.3f %.3f %.3f %.3f\n", $16, $29, $42, $55, $68}' phot > phot.mags
awk '{printf "%.3f %.3f %.3f %.3f %.3f\n", $18, $31, $44, $57, $70}' phot > phot.errs
awk '{printf "%.2f %.2f %.2f %.2f %.2f %.2f\n", $19, $32, $45, $58, $71, $5}' phot > phot.chi
awk '{printf "%.1f %.1f %.1f %.1f %.1f %.1f\n", $20, $33, $46, $59, $72, $6}' phot > phot.snr
awk '{printf "%.3f %.3f %.3f %.3f %.3f %.3f\n", $21, $34, $47, $60, $73, $7}' phot > phot.sharp
awk '{printf "%.3f %.3f %.3f %.3f %.3f %.3f\n", $22, $35, $48, $61, $74, $8}' phot > phot.round
awk '{printf "%.3f %.3f %.3f %.3f %.3f %.3f\n", $23, $36, $49, $62, $75, $10}' phot > phot.crowd
awk '{printf "%d %d %d %d %d %d\n", $24, $37, $50, $63, $76, $11}' phot > phot.flags

Then use this script as, e.g.
./phot_cmd.py phot
./phot_cmd.py $PATH/phot
./phot_cmd.py $PATH1/phot, $PATH2/phot, $PATH3/phot
./phot_cmd.py phot -x1 -0.5 -x2 3.5 -y1 28.5 -y2 15 -bin 0.1 -lvl [100,150,200,250,300,400,600,800,1200,1600,2000] -thr 150 -pid '10x10' -grid 5 -targ Halo

Also see: multi_cmd.py, test_cull.py
'''
from multi_cmd import *
del DoAll

matplotlib.use('Agg')
plt.ioff()

'''Create worker pool'''
if __name__ != '__main__':
    cpu_pool = concurrent.futures.ThreadPoolExecutor(max_workers=cpu_count())
else:
    cpu_pool = concurrent.futures.ProcessPoolExecutor(max_workers=cpu_count())

filters   = np.array(['Z087','Y106','J129','H158','F184'])


def DoAll(filenames,xlim1=[],xlim2=[],ylim1=[],ylim2=[],binsize=[],levels=[],threshold=[],pid=[],target=[],n=4,fmt='png'):
    for filename in filenames:
        fileroot,filename = get_fileroot(filename)
        vega_mags,mag_errors = read_ascii_col(fileroot+filename,['mags','errs'],len(filters))
        xy,radec = read_ascii_col(fileroot+filename,['xy','radec'],2)
        del radec
        do_cmds = lambda a,b: start_cmds(vega_mags,mag_errors,xy,filters,\
                                         fileroot,filename,\
                                         xlim1,xlim2,ylim1,ylim2,binsize,levels,\
                                         threshold,pid,target,n,\
                                         good=a,flag=b,fmt=fmt)
        do_cmds('.st',[])
        
        Flags,Sharp,Round,Chi,SNR,Crowd = read_ascii_col(fileroot+filename,\
            ['flags','sharp','round','chi','snr','crowd'],len(filters)+1)
        flag = cull_phot(vega_mags,mag_errors,xy,Flags,Sharp,Round,Chi,SNR,Crowd)
        del Flags,Sharp,Round,Chi,SNR,Crowd
        
        do_cmds('.gst',flag)
        del vega_mags,mag_errors,xy
        
    return None


def start_cmds(mags,errors,XY,filters,fileroot,filename,xlim1,xlim2,ylim1,ylim2,binsize,levels,threshold,pid,target,n,good='',flag=[],fmt='png'):
    X,Y =  XY[0,:], XY[1,:]
    del XY
    for i in range(len(filters)-1):
        for j in range(i,len(filters)-1):
            f1,f2=filters[i],filters[j+1]
            print(' '.join(('Got filters ',f1,' and ',f2)))
            outfile = filename+'_'+f1+'_'+f2+good
            m1,m2,e1,e2,x,y=mags[i],mags[j+1],errors[i],errors[j+1],X,Y
            if len(flag):
                in1 = (flag[i])&(flag[j+1])
                m1,m2,e1,e2,x,y =  m1[in1],m2[in1],e1[in1],e2[in1], x[in1], y[in1]
            cpu_pool.submit(plot_cmds,x,y,m1,m2,e1,e2,f1,f2,fileroot,outfile,\
                            xlim1,xlim2,ylim1,ylim2,binsize,levels,threshold,pid,target,n,fmt)
    return None


def cull_phot(vega_mags,mag_errors,xy,Flags,Sharp,Round,Chi,SNR,Crowd):
    flag = []
    for i in range(vega_mags.shape[0]):
        _tmp = (Flags[i]==0)&(Flags[-1]==1)
        flag.append(_tmp)
    return np.array(flag)


def read_ascii_col(filename,suffix,ncol):
    if isinstance(ncol,int):
        ncol = range(ncol)
    for suff in suffix:
        _tmp, _file = [], '.'.join((filename,suff))
        print('Reading in:\n', _file)
        _data = ascii.read(_file)
        for i in ncol:
            _tmp.append(_data['col'+str(i+1)])
        yield np.array(_tmp)

        
if __name__ == '__main__':
    tic = time.time()
    assert 3/2 == 1.5, 'Not running Python3 may lead to wrong results'
    args = parse_all()
    DoAll(filenames=args.filenames,xlim1=args.xlim1,xlim2=args.xlim2,ylim1=args.ylim1,ylim2=args.ylim2,binsize=args.binsize,\
          levels=args.levels,threshold=args.threshold,pid=args.pid,target=args.target,n=args.grid,fmt=args.fmt)
    cpu_pool.shutdown(wait=True)
    print('\n\nCompleted in %.3f seconds \n' % (time.time()-tic))
