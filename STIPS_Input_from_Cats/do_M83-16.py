#! /usr/bin/env python
'''
Ingest HST B-I + pseudo F110-F160 catalogs, produce HST F606 and WFI 
Z087, Y105, H158 mags from linear fits on log-log SEDs 
'''
from astropy.io import ascii
from astropy.table import Table
import numpy as np

from astropy import constants as const
h = const.h.cgs.value
c = const.c.cgs.value

def DoAll():
    data  = ascii.read('M83-Comb1_4mags.txt')
    m1, m2, m3, m4, ra, dec =  data['m1'],  data['m2'], data['m3'], data['m4'], data['ra'], data['dec']
    del data

    # HST UVIS-1/IR Vega zero points in Jy, and corresponding lambda_eff
    fo = np.array([4196.2, 2439.35, 1738.4, 1138.06]) # F438W, F814W, F110W, F160W
    wv1 = np.array([0.43151,0.790114,1.102969,1.523589])*1e-4 # cm
    
    # Flux per freq bin in ergs/s/cm2/Hz
    m = np.array([m1,m2,m3,m4]).T
    f_nu = 10**(m/(-2.5)) * fo * 1e-23

    # log of Counts per lambda bin in photons/s/cm2/cm
    N_lam = np.log10(f_nu/(wv1*h))
    wv1 = np.log10(wv1)
    del m, f_nu

    # Effective Area * Relative Bandpass, and lambda_pivot for the notional ZYH filters + F606-like f_1
    Arel = np.array([0.2107,0.5663,0.5856,0.5686])*1e4   # m^2 > cm^2
    wv2  = np.array([0.6342,0.8758,1.0671,1.5909])*1e-4  # um  > cm
    wv2 = np.log10(wv2)
    
    tmp = np.array([np.polyfit(wv1[0:2], [N_lam[:,0][i], N_lam[:,1][i]], 1) for i in range(N_lam.shape[0])])
    A, B = tmp[:,0], tmp[:,1]
    N0 = 10**(A*wv2[0]+B) * 10**wv2[0] * Arel[0]
    del tmp, A, B

    tmp = np.array([np.polyfit(wv1[1:3], [N_lam[:,1][i], N_lam[:,2][i]], 1) for i in range(N_lam.shape[0])])
    A, B = tmp[:,0], tmp[:,1] 
    N1, N2 = 10**(A*wv2[1]+B) * 10**wv2[1] * Arel[1], 10**(A*wv2[2]+B) * 10**wv2[2] * Arel[2]
    del tmp, A, B

    tmp = np.array([np.polyfit(wv1[2:4], [N_lam[:,2][i], N_lam[:,3][i]], 1) for i in range(N_lam.shape[0])])
    A, B = tmp[:,0], tmp[:,1] 
    N3 = 10**(A*wv2[3]+B) * 10**wv2[3] * Arel[3]
    del tmp, A, B

    id = np.arange(ra.size)+1
    ones = np.ones_like(id)
    typ = np.repeat(np.array(['point']),id.size)
    cmnt = np.repeat(np.array(['comment']),id.size)

    tab0 = [id, ra, dec, N0, typ, ones, ones, ones, ones, cmnt]
    tab1 = [id, ra, dec, N1, typ, ones, ones, ones, ones, cmnt]
    tab2 = [id, ra, dec, N2, typ, ones, ones, ones, ones, cmnt]
    tab3 = [id, ra, dec, N3, typ, ones, ones, ones, ones, cmnt]

    nms = ('id', 'ra', 'dec', 'flux', 'type', 'n', 're', 'phi', 'ratio', 'notes')
                                                                                            
    fmt = {'id':'%10d', 'ra':'%10.5f', 'dec':'%10.5f', 'flux':'%15.5f', 'type':'%8s', \
           'n':'%8.1f', 're':'%8.1f', 'phi':'%8.1f', 'ratio':'%8.1f', 'notes':'%8s'}  

    t0 = Table(tab0, names=nms)
    t1 = Table(tab1, names=nms)
    t2 = Table(tab2, names=nms)
    t3 = Table(tab3, names=nms)

    ascii.write(t0, 'list0.Comb1.tbl', format='fixed_width', delimiter='', formats=fmt)
    ascii.write(t1, 'list1.Comb1.tbl', format='fixed_width', delimiter='', formats=fmt)
    ascii.write(t2, 'list2.Comb1.tbl', format='fixed_width', delimiter='', formats=fmt)
    ascii.write(t3, 'list3.Comb1.tbl', format='fixed_width', delimiter='', formats=fmt)    

if __name__ == '__main__':
    tmp = 3/2
    print('This should be 1.5: %.3f' % tmp)
    DoAll()
