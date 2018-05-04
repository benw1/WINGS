#! /usr/bin/env python
'''
Produce a blackbody curve *** AB *** color look-up table, going from 
[475-814] to [475-X], where X are the J_H_7_1 filters
'''
import numpy as np
from astropy.io import ascii
from astropy.table import Table
from astropy import constants as const
c = const.c.cgs.value
h = const.h.cgs.value
k = const.k_B.cgs.value

def DoAll():
    # lambda_cen in cm; 475W,814W, J_H_7_1
    wv = np.array([0.476873,0.782072,0.590979,0.817739,1.022070,1.240151,1.535107,1.830465,1.326561])
    wv = wv*1e-4
    T = 10**np.linspace(3,5,1e4)

    col = [AB_color(wv[0],wv[i+1],T) for i in range(wv.size-1)]

    tab = [T,col[0],col[1],col[2],col[3],col[4],col[5],col[6],col[7]]
    nms = ('T','c0I','c01','c02','c03','c04','c05','c06','c07')
    fmt = {'T':'%5.2f','c0I':'%10.3f','c01':'%10.3f','c02':'%10.3f','c03':'%10.3f','c04':'%10.3f','c05':'%10.3f','c06':'%10.3f','c07':'%10.3f'}

    t = Table(tab, names=nms)

    ascii.write(t, 'AB_color1.txt', format='fixed_width', delimiter='', formats=fmt)


def BB_lam(lam,T):
    return 2*h*c**2 / (lam**5 * (np.exp(h*c / (lam*k*T)) - 1))


def AB_color(l1,l2,T):
    f1,f2 = BB_lam(l1,T), BB_lam(l2,T)
    return -2.5*np.log10((f1/f2)*(l1/l2)**2)


if __name__ == '__main__':
    tmp = 3/2
    print('This should be 1.5: %.3f' % tmp)

    DoAll()

