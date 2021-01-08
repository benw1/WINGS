#! /usr/bin/env python
import numpy as np
import argparse,os,subprocess
from wingtips import WingTips as wtips
import gc

pix = 0.11 #arcsec per pixel
#imagesize = 2048.0 #just use central 2048 pix 
imagesize = 4096.0 #use full image 
area = imagesize**2  * pix**2
pixarea = pix**2
#racent = 325.65
#deccent=-27.21
racent = 25.65
deccent=-37.21
def read_match(file,cols):
    data = np.loadtxt(file)
    np.random.shuffle(data)
    print(data[5:10,:])
    nstars = len(data[:,0])
    tot_dens = np.float(nstars)/area
    print("MAX TOTAL DENSITY = ",tot_dens)
    count = -1
    for col in (cols):
        count += 1
        if (col == 'H158'):
            print("H is column ",count)
            hcol = count
        if (col == 'X625'):
            print("X is column ",count)
            xcol = count
        if (col == 'Y106'):
            print("Y is column ",count)
            ycol = count
        if (col == 'Z087'):
            print("Z is column ",count)
            zcol = count
        if (col == 'J129'):
            print("J is column ",count)
            jcol = count
        if (col == 'F184'):
            print("F is column ",count)
            fcol = count
    h = data[:,hcol]
    htot_keep = (h > 23.0) & (h < 24.0)
    hkeep = h[htot_keep]
    htot = len(hkeep)
    max_den = np.float(htot)/area
    del h
    print("MAX H(23-24) DENSITY = ",max_den)
    for i in range(1,2):
        totstars = np.float(nstars) * 10**(-0.1*np.float(i)) 
        allind = np.rint(np.arange(totstars-1) * np.float(nstars)/totstars)
        mydata = data[allind.astype(int),:]
        mytot = len(mydata[:,0])
        mytot_dens = mytot/area
        myh = mydata[:,hcol]
        mykeep =  (myh > 23.0) & (myh < 24.0)
        myhkeep = myh[mykeep]
        del myh
        hden = np.float(len(myhkeep))/area
        print(10**(-0.1*np.float(i))," TOTAL DENSITY = ",mytot/area)
        print(10**(-0.1*np.float(i))," H(23-24) DENSITY = ",hden)

        M1, M2, M3, M4, M5 =  mydata[:,zcol], mydata[:,ycol], mydata[:,jcol], mydata[:,hcol],mydata[:,fcol]
        radist = np.abs(1/((mytot_dens**0.5)*np.cos(deccent*3.14159/180.0)))/3600.0
        decdist = (1/mytot_dens**0.5)/3600.0
        #print('RA',radist,'DEC',decdist)
        coordlist = np.arange(np.rint(np.float(len(M2))**0.5)+1)
        np.random.shuffle(coordlist)
        #print(radist,decdist)
        ra = 0.0
        dec = 0.0
        for k in range(len(coordlist)):
            ra = np.append(ra,radist*coordlist+racent-(pix*imagesize/7200.0))
            dec = np.append(dec,np.repeat(decdist*coordlist[k]+deccent-(pix*imagesize/7200.0),len(coordlist)))
        ra = ra[1:len(M1)+1]
        dec = dec[1:len(M1)+1]
        print(len(ra),len(M1))
        print("MIN AND MAX RA AND DEC ARE: ",np.min(ra),np.max(ra),np.min(dec),np.max(dec))
        M = np.array([M1,M2,M3,M4,M5]).T
        del M1,M2,M3,M4,M5
        file1 = file.split('.')
        file2 = '.'.join(file1[0:len(file1)-1])
        file3 = file2+str(np.around(hden,decimals=5))+'.'+file1[-1]
        if i==1:
            galradec = getgalradec(file3,ra,dec,M)
        write_stips(file3,ra,dec,M,galradec)
        del M
        gc.collect()

def getgalradec(infile,ra,dec,M):
    filt = 'Z087'
    ZP_AB = np.array([26.365,26.357,26.320,26.367,25.913])
    fileroot=infile
    starpre = '_'.join(infile.split('.')[:-1])
    filedir = infile.split('/')[0]+'/'
    outfile = starpre+'_'+filt+'.tbl'
    outfilename = outfile.split('/')[-1]
    flux    = wtips.get_counts(M[:,0],ZP_AB[0])
    wtips.from_scratch(flux=flux,ra=ra,dec=dec,outfile=outfile)
    stars = wtips([outfile])
    galaxies = wtips([filedir+filt+'.txt']) # this file will be provided pre-made
    radec = galaxies.random_radec_for(stars)
    return radec
    
def write_stips(infile,ra,dec,M,galradec):
    filternames   = ['Z087','Y106','J129','H158','F184']
    ZP_AB = np.array([26.365,26.357,26.320,26.367,25.913])
    fileroot=infile
    starpre = '_'.join(infile.split('.')[:-1])
    filedir = infile.split('/')[0]+'/'
    for j,filt in enumerate(filternames):
        
        outfile = starpre+'_'+filt[0]+'.tbl'
        outfilename = outfile.split('/')[-1]
        flux    = wtips.get_counts(M[:,j],ZP_AB[j])
        print(M[-1,j])
        #print(flux,ra,dec)
        # This makes a stars only input list
        wtips.from_scratch(flux=flux,ra=ra,dec=dec,outfile=outfile)
        stars = wtips([outfile])
        subprocess.run(['gzip',outfile], stdout=subprocess.PIPE)
        galaxies = wtips([filedir+filt+'.txt']) # this file will be provided pre-made
        galaxies.flux_to_Sb()                             # galaxy flux to surface brightness
        galaxies.replace_radec(galradec)                     # distribute galaxies across starfield
        stars.merge_with(galaxies)                        # merge stars and galaxies list
        outfile = filedir+'Mixed'+'_'+outfilename
        stars.write_stips(outfile,ipac=True)
        with open(outfile, 'r+') as f:
            content = f.read()
            f.seek(0, 0)
            f.write('\\type = internal' + '\n'  +
                 '\\filter = ' + str(filt) +'\n' + 
                 '\\center = (' + str(stars.center[0]) +
                 '  ' + str(stars.center[1]) + ')\n' +
                 content)
        f.close()
        subprocess.run(['gzip',outfile], stdout=subprocess.PIPE)
        del stars
        del galaxies
        gc.collect()
#read_match('wfirst_phot/fake_25.5.out',['X625','Z087','Y106','J129','H158','F184'])
read_match('wfirst_phot/fake_test.out',['X625','Z087','Y106','J129','H158','F184'])
    
       
