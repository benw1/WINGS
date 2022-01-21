#! /usr/bin/env python
'''
Ingest HST and WFI mags from Leo's simulation 
and make CMD
'''
def DoAll():
    from astropy.io import ascii
    from astropy.table import Table

    import numpy as np

    data = ascii.read('tri_acs_wfc3_wfirst_J_H_7_1_extgal35.dat')

    m1,m2,m3,m4,m5,m6,w1,w2,w3,w4,w5,w6,w7 = data['F475Wmag'], data['F555Wmag'], \
         data['F606Wmag'], data['F814Wmag'], data['F110Wmag'], data['F160Wmag'], \
         data['f_1mag'], data['f_2mag'], data['f_3mag'], data['f_4mag'], data['f_5mag'], \
         data['f_6mag'],  data['f_7mag']
    del data

    # DoFig(m1,m4,'cmd4.pdf')
    # DoFig(w2,w5,'cmd5.pdf')
    
    dx = np.array([-0.102,-0.028,0.081,0.418,0.76,1.252,0.069,0.427,0.611,0.897,1.251,1.547,0.967])

    m1,m2,m3,m4,m5,m6,w1,w2,w3,w4,w5,w6,w7 = m1-dx[0],m2-dx[1],m3-dx[2],m4-dx[3],m5-dx[4],m6-dx[5],\
                                             w1-dx[6],w2-dx[7],w3-dx[8],w4-dx[9],w5-dx[10],w6-dx[11],w7-dx[12]

    DoFig(m1,m4,'cmd6.pdf')
    # del m1,m2
    # data = ascii.read('M83-Comb.txt')
    # m1, err1, m2, err2, ra, dec = data['m1'], data['err1'], data['m2'], data['err2'], data['ra'], data['dec']
    # del data
    # DoFig(m1,m2,'cmd7.pdf')
    
def DoFig(m1,m2,filename):
    import matplotlib.pyplot as plt

    plt.rc("font", size=10, family='serif', weight='bold')
    plt.rc("axes", labelsize=7, titlesize=20)
    plt.rc("xtick", labelsize=7.5)
    plt.rc("ytick", labelsize=7.5)
    plt.rc("legend", fontsize=10)

    fig1 = plt.figure(figsize = ((5,5)))
    fig1.suptitle('Leo_Tri')

    m1m2 = m1-m2

    x,y = m1m2, m2
    plt.plot(x,y,'rx')
    plt.xlim(-3,7)
    plt.ylim(30,18)
    plt.xlabel('m1-m2',fontsize=10)
    plt.ylabel(r'm2',fontsize=10)
    plt.savefig(filename)


if __name__ == '__main__':
    tmp = 3/2
    print('This should be 1.5: %.3f' % tmp)
    DoAll()
