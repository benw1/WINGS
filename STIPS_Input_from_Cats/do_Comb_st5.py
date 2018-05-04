#! /usr/bin/env python
'''
Ingest HST B-I optical catalog ascii files, then
write out B&I err<0.5 sources only 
'''
def DoAll():
    from astropy.io import ascii
    from astropy.table import Table

    import numpy as np

    data = ascii.read('M83-F9.st.txt')
    ra,dec,m1,err1,m2,err2 = data['col1'],data['col2'],data['col3'],data['col4'],data['col5'],data['col6']
    del data

    in1 = [i for i in range(m1.size) if((err1[i]<0.5)&(err2[i]<0.5))]
    ra,dec,m1,m2,err1,err2 = np.array(ra[in1[:]]), np.array(dec[in1[:]]), \
                             np.array(m1[in1[:]]), np.array(m2[in1[:]]), \
                             np.array(err1[in1[:]]), np.array(err2[in1[:]])    
    del in1

    tab = [ra,dec,m1,err1,m2,err2]
    nms = ('ra','dec','m1','err1','m2','err2')
    fmt = {'ra':'%10.5f', 'dec':'%10.5f', 'm1':'%8.3f', 'err1':'%8.3f', 'm2':'%8.3f', 'err2':'%8.3f'}
    t = Table(tab, names=nms)

    ascii.write(t, 'M83-F9.st5.txt', format='fixed_width', delimiter='', formats=fmt)

if __name__ == '__main__':
    tmp = 3/2
    print('This should be 1.5: %.3f' % tmp)
    DoAll()

