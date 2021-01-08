#! /usr/bin/env python
'''
Ingest HST optical catalogs, make color lookup table
'''
def DoAll():
    from astropy.io import fits
    from astropy.io import ascii
    from astropy.table import Table
    import numpy as np

    data = fits.open('12075_M31-B08-F15.gst.fits')[1].data
    m1, m2, m3, m4 = data['F475W_VEGA '], data['F814W_VEGA '], data['F110W_VEGA '], data['F160W_VEGA ']
    del data

    in1 = [i for i in range(m1.size) if((m1[i]<30)&(m2[i]<30)&(m3[i]<30)&(m4[i]<30))]
    mag1,mag2,mag3,mag4 = np.array(m1[in1[:]]), np.array(m2[in1[:]]), np.array(m3[in1[:]]), np.array(m4[in1[:]])
    del in1
    DoFig(mag1,mag2,'cmd1.pdf')
    
    in1 = [i for i in range(m1.size) if((m1[i]<30)&(m2[i]<30)&(m3[i]<30))]
    mag1,mag2,mag3 = np.array(m1[in1[:]]), np.array(m2[in1[:]]), np.array(m3[in1[:]])
    del in1
    DoFig(mag1,mag2,'cmd2.pdf')
    
    in1 = [i for i in range(m1.size) if((m1[i]<30)&(m2[i]<30)&(m4[i]<30))]
    mag1,mag2,mag4 = np.array(m1[in1[:]]), np.array(m2[in1[:]]), np.array(m4[in1[:]])
    del in1
    DoFig(mag1,mag2,'cmd3.pdf')

def tempOut():
    c12,c13,c14 = m1-m2,m1-m3,m1-m4

    tab = [c12,c13,c14]
    nms = ('c12','c13','c14')
    fmt = {'c12':'%10.7f','c13':'%10.7f','c14':'%10.7f'}

    t = Table(tab, names=nms)

    ascii.write(t, 'M31_col_B08-F15.txt', format='fixed_width', delimiter='', formats=fmt)


def DoFig(m1,m2,filename):
    import matplotlib.pyplot as plt

    plt.rc("font", size=10, family='serif', weight='bold')
    plt.rc("axes", labelsize=7, titlesize=20)
    plt.rc("xtick", labelsize=7.5)
    plt.rc("ytick", labelsize=7.5)
    plt.rc("legend", fontsize=10)

    fig1 = plt.figure(figsize = ((5,5)))
    fig1.suptitle('M31')

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

