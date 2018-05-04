#! /usr/bin/env python
'''
Ingest HST optical catalogs fits tables, produce simple 
ascii files with RA, DEC and B-I band mags/err
'''
def DoAll():
    from astropy.io import fits
    from astropy.io import ascii
    from astropy.table import Table

    import numpy as np

    data = fits.open('12513_M83-F7.st.fits')[1].data
    ra, dec, m1, m2, err1, err2 = data['RA      '], data['DEC     '], \
                                  data['F438W_VEGA '], data['F814W_VEGA '], \
                                  data['F438W_ERR '], data['F814W_ERR ']
    del data

    in1 = [i for i in range(m1.size) if((m1[i]<30)&(m2[i]<30))]
    ra,dec,m1,m2,err1,err2 = np.array(ra[in1[:]]), np.array(dec[in1[:]]), \
                             np.array(m1[in1[:]]), np.array(m2[in1[:]]), \
                             np.array(err1[in1[:]]), np.array(err2[in1[:]])
    
    tab = [ra,dec,m1,err1,m2,err2]
    nms = ('ra','dec','m1','err1','m2','err2')
    fmt = {'ra':'%10.5f', 'dec':'%10.5f', 'm1':'%8.4f', 'err1':'%8.4f', 'm2':'%8.4f', 'err2':'%8.4f'}
    t = Table(tab, names=nms)

    ascii.write(t, 'M83-F7.st.txt', format='fixed_width', delimiter='', formats=fmt)

if __name__ == '__main__':
    tmp = 3/2
    print('This should be 1.5: %.3f' % tmp)
    DoAll()
