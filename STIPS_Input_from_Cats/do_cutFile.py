#! /usr/bin/env python
'''
Ingest an ascii file, keeps every N-th entry
'''
def DoAll():

    N = 10
    
    from astropy.io import ascii
    from astropy.table import Table

    import numpy as np

    F4 = ascii.read('M83-F4.st5.txt')
    F7 = ascii.read('M83-F7.st5.txt')
    F8 = ascii.read('M83-F8.st5.txt')

    in4 = range(0,F4['ra'].size,N)
    in7 = range(0,F7['ra'].size,N)
    in8 = range(0,F8['ra'].size,N)

    tab1 = [F4['ra'][in4],F4['dec'][in4],F4['m1'][in4],F4['err1'][in4],F4['m2'][in4],F4['err2'][in4]]
    tab2 = [F7['ra'][in7],F7['dec'][in7],F7['m1'][in7],F7['err1'][in7],F7['m2'][in7],F7['err2'][in7]]
    tab3 = [F8['ra'][in8],F8['dec'][in8],F8['m1'][in8],F8['err1'][in8],F8['m2'][in8],F8['err2'][in8]]    

    
    nms = ('ra','dec','m1','err1','m2','err2')
    fmt = {'ra':'%10.5f', 'dec':'%10.5f', 'm1':'%8.3f', 'err1':'%8.3f', 'm2':'%8.3f', 'err2':'%8.3f'}
    
    t1,t2,t3 = Table(tab1, names=nms), Table(tab2, names=nms), Table(tab3, names=nms)

    ascii.write(t1, 'test1.txt', format='fixed_width', delimiter='', formats=fmt)
    ascii.write(t2, 'test2.txt', format='fixed_width', delimiter='', formats=fmt)
    ascii.write(t3, 'test3.txt', format='fixed_width', delimiter='', formats=fmt)

if __name__ == '__main__':
    tmp = 3/2
    print('This should be 1.5: %.3f' % tmp)
    import time
    tic = time.time()
    DoAll()
    toc = time.time()
    print(toc-tic)
