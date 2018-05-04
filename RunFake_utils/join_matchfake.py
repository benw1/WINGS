#!/usr/bin/env python
from astropy.io import ascii
from astropy.table import Table

def DoAll(filename1='14266_NGC6946BH1_F438W.gst',filename2='14266_NGC6946BH1_F606W.gst',
          filename3='14266_NGC6946BH1_F814W.gst',filename4='14266_NGC6946BH1_F110W.gst',
          filename5='14266_NGC6946BH1_F160W.gst',outfile='test'):

    data1 = ascii.read(filename1+'.matchfake')
    m1,diff1 = data1['col1'], data1['col2']

    data2 = ascii.read(filename2+'.matchfake')
    m2,diff2 = data2['col1'], data2['col2']

    data3 = ascii.read(filename3+'.matchfake')
    m3,diff3 = data3['col1'], data3['col2']

    data4 = ascii.read(filename4+'.matchfake')
    m4,diff4 = data4['col1'], data4['col2']

    data5 = ascii.read(filename5+'.matchfake')
    m5,diff5 = data5['col1'], data5['col2']


    tab1 = [m1,m2,m3,m4,m5,diff1,diff2,diff3,diff4,diff5]
    nms1 = ('m1','m2','m3','m4','m5','diff1','diff2','diff3','diff4','diff5')
    fmt1 = {'m1':'%8.3f', 'm2':'%8.3f', 'm3':'%8.3f', 'm4':'%8.3f', 'm5':'%8.3f',\
            'diff1':'%8.3f', 'diff2':'%8.3f', 'diff3':'%8.3f', 'diff4':'%8.3f', 'diff5':'%8.3f'}
    t1 = Table(tab1, names=nms1)
    ascii.write(t1, outfile+'5.txt', format='fixed_width', delimiter='', formats=fmt1)

    tab2 = [m2,m3,diff2,diff3]
    nms2 = ('m2','m3','diff2','diff3')
    fmt2 = {'m2':'%8.3f', 'm3':'%8.3f', 'diff2':'%8.3f', 'diff3':'%8.3f'}
    t2 = Table(tab2, names=nms2)
    ascii.write(t2, outfile+'2.txt', format='fixed_width', delimiter='', formats=fmt2)

    tab3 = [m1,m2,diff1,diff2]
    nms3 = ('m1','m2','diff1','diff2')
    fmt3 = {'m1':'%8.3f', 'm2':'%8.3f', 'diff1':'%8.3f', 'diff2':'%8.3f'}
    t3 = Table(tab3, names=nms3)
    ascii.write(t3, outfile+'1.txt', format='fixed_width', delimiter='', formats=fmt3)

    tab4 = [m1,m2,m3,diff1,diff2,diff3]
    nms4 = ('m1','m2','m3','diff1','diff2','diff3')
    fmt4 = {'m1':'%8.3f', 'm2':'%8.3f', 'm3':'%8.3f',\
            'diff1':'%8.3f', 'diff2':'%8.3f', 'diff3':'%8.3f'}
    t4 = Table(tab4, names=nms4)
    ascii.write(t4, outfile+'3.txt', format='fixed_width', delimiter='', formats=fmt4)



if __name__ == '__main__':
    tmp = 3/2
    print(10*'\n'+'Python3: This should be 1.500 = %.3f\n' % tmp)
    import time
    tic = time.time()

    DoAll(filename1='14266_NGC6946BH1_F438W.gst',filename2='14266_NGC6946BH1_F606W.gst',
          filename3='14266_NGC6946BH1_F814W.gst',filename4='14266_NGC6946BH1_F110W.gst',
          filename5='14266_NGC6946BH1_F160W.gst',outfile='Fake_gst')
    DoAll(filename1='14266_NGC6946BH1_F438W.st',filename2='14266_NGC6946BH1_F606W.st',
          filename3='14266_NGC6946BH1_F814W.st',filename4='14266_NGC6946BH1_F110W.st',
          filename5='14266_NGC6946BH1_F160W.st',outfile='Fake_st')

    tmp = time.time()-tic
    print('Completed in %.3f seconds\n' % tmp)
