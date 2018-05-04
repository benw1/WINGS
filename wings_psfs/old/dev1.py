#!/usr/bin/env python
import numpy as np
from webbpsf.wfirst import WFI

chips = ['SCA01','SCA09','SCA18']
filternames = ['Z087','Y106','J129','H158','F184']
grid = 5
oversample = 11

def DoAll(chips=['SCA01'],filternames=['Z087'],grid=5,over=11,outprefix='WFI'):
    if grid>1:
        x0 = 4096/(field+1)
        x1 = x0*field
        xcen,ycen = np.linspace(x0,x1,field), np.linspace(x0,x1,field)
        positions = []
        for i in range(field):
            for j in range(field):
                positions.append(xcen[i],ycen[j])
    else:
        positions = [(2048,2048)]
    for det in chips:
        for filt in filternames:
            for pos in positions:
                print(pos)
                write_psf(det=det,filt=filt,pos=pos,over=oversample)
    return None


def write_psf(det='SCA01',filt='Z087',pos=(2048,2048),over=11,outprefix='WFI',wfi=WFI()):
    outpre = '_'.join([outprefix,det,filt,'pos',str(pos[0]),str(pos[1])])
    outfile0 = '_'.join([outpre,str(over)+'x','oversampled.fits'])
    outfile1 = '_'.join([outpre,'native_pixel.fits'])
    wfi.detector = det
    wfi.detector_position = pos
    wfi.filter = filt
    psf = wfi.calc_psf(oversample=over)
    psf[0].writeto(outfile0,overwrite=True)
    psf[1].writeto(outfile1,overwrite=True)
    return None



if __name__ == '__main__':
    import time
    tic = time.time()
    assert 3/2 == 1.5, 'Not running Python3 may lead to wrong results'
    DoAll(chips,[filternames[0]],grid=1,over=oversample)
    print('\n\nCompleted in %.3f seconds \n' % (time.time()-tic))
