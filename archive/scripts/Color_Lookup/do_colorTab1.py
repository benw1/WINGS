#! /usr/bin/env python
'''
Produce a blackbody curve color (mag1-mag2) look-up table
'''
import numpy as np
from astropy.io import ascii
from astropy.table import Table
from astropy import constants as const

c = const.c.cgs.value
h = const.h.cgs.value
k = const.k_B.cgs.value

def DoAll():
    # HST UVIS-1/IR zero points in Jy
    fo= np.array([4196.2, 2439.35, 1738.4, 1138.06]) # F438W, F814W, F110W, F160W

    # Effective band centers in cm
    wv = np.array([0.43151e-4, 0.790114e-4, 1.102969e-4, 1.523589e-4])

    T = 10**np.linspace(3,5,1e4)
    
    c12 = BB_color(wv[0],wv[1],fo[0],fo[1],T)
    c13 = BB_color(wv[0],wv[2],fo[0],fo[2],T)
    c14 = BB_color(wv[0],wv[3],fo[0],fo[3],T)

    tab = [T,c12,c13,c14]
    nms = ('T','c12','c13','c14')
    fmt = {'T':'%5.2f','c12':'%10.7f','c13':'%10.7f','c14':'%10.7f'}

    t = Table(tab, names=nms)

    ascii.write(t, 'BB_color2.txt', format='fixed_width', delimiter='', formats=fmt)


def BB_lam(lam,T):
    return 2*h*c**2 / (lam**5 * (np.exp(h*c / (lam*k*T)) - 1))

def BB_color(l1,l2,fo1,fo2,T):
    f1,f2 = BB_lam(l1,T), BB_lam(l2,T)
    return -2.5*np.log10((fo2/fo1)*(f1/f2)*(l1/l2)**2)



if __name__ == '__main__':
    tmp = 3/2
    print('This should be 1.5: %.3f' % tmp)

    DoAll()


# return -2.5*np.log10((fo2/fo1) * (l2/l1)**4 * np.exp(((h*c)/(k*T))/((1/l2)-(1/l1))) )
