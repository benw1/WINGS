#! /usr/bin/env python
'''
Read in stellar catalogs from simulations with Absolute AB mags and 
background galaxy catlogs. Using the Wingtips lib, produce mixed list 
of objects including stars and appropriate sampling of background 
galaxies in STIPS input format
'''

from wingtips import WingTips as wtips
from wingtips import time, np, ascii

filenames = ['h15.shell.1Mpc.in', 'h15.shell.3Mpc.in', 'h15.shell.5Mpc.in', 'h15.shell.10Mpc.in']
ZP_AB = np.array([26.365,26.357,26.320,26.367,25.913])
filternames   = ['Z087','Y106','J129','H158','F184']

def make_stips():
    for i,infile in enumerate(filenames):
        dist = float(infile.split('.')[2][:-3])
        starpre = '_'.join(infile.split('.')[:-1])
        data = ascii.read(infile)
        print('\nRead in %s \n' % infile)
        RA, DEC, M1, M2, M3, M4, M5 = \
            data['col1'], data['col2'], data['col3'], data['col4'],\
            data['col5'], data['col6'], data['col7']        
        M = np.array([M1,M2,M3,M4,M5]).T
        temp = [wtips.from_scratch(\
                    flux=wtips.get_counts(M[:,j],ZP_AB[j],dist=dist),\
                    ra=RA,dec=DEC,outfile=starpre+'_'+filt[0]+'.tbl')\
                for j,filt in enumerate(filternames)]
    return None

def mix_stips(filternames=filternames,filenames=filenames,outprefix='Mixed'):
    galaxies = []
    for i,infile in enumerate(filenames):
        starpre = '_'.join(infile.split('.')[:-1])
        radec = []
        for j,filt in enumerate(filternames):
            stars = wtips([starpre+'_'+filt[0]+'.tbl'])
            if i==0:
                galaxies.append(wtips([filt+'.txt']))
                galaxies[j].flux_to_Sb()
            if len(radec)==0:
                radec = galaxies[j].random_radec_for(stars)
            galaxies[j].replace_radec(radec)
            stars.merge_with(galaxies[j])
            outfile='_'.join((outprefix,starpre,filt[0]))+'.tbl'
            stars.write_stips(outfile,ipac=True)
            with open(outfile, 'r+') as f:
                content = f.read()
                f.seek(0, 0)
                f.write('\\type = internal' + '\n'  + '\\filter = ' + str(filt) +'\n' + \
                    '\\center = (' + str(stars.center[0]) + '  ' + str(stars.center[1]) + ')\n' + content)
    return None

if __name__ == '__main__':
    tic = time.time()
    assert 3/2 == 1.5, 'Not running Python3 may lead to wrong results'
    make_stips()
    mix_stips()
    print('\n\nCompleted in %.3f seconds \n' % (time.time()-tic))
