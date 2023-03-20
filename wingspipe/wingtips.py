#! /usr/bin/env python
"""
WFIRST Infrared Nearby Galaxies Test Image Product Simulator
Produces input files for the WFIRST STIPS simulator
"""
# Error message when initializing pipeline:
"""
 /Users/kathrynwynn/miniconda3/envs/stips/lib/python3.10/site-packages/wpipe/Task.py:416: UserWarning: Task /Users/kathrynwynn/Documents/ASTR499/Minipipe/TestFolder/build/wingtips_copy.py cannot be registered: no 'register' function
 warnings.warn("Task " + self.pipeline.software_root + '/' + self.name +

In my wpipe/Task.py there is no register function, also the error message simply cuts off without closing parenthesis.
"""

import time
import subprocess
import resource
import gc
import numpy as np
from astropy import wcs
from astropy.io import fits, ascii
from astropy.table import Table
import dask.dataframe as dd 

import wpipe as wp

try:
    this_job = wp.ThisJob
    specialprint = this_job.logprint
except AttributeError:
    specialprint = print


class WingTips:
    """
    Initialize WingTips object
    """

    def __init__(self, infile=[], center=[0, 0], **kwargs):
        gc.collect()
        specialprint("Starting WingTips __init__ %f MB" % (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024))
        if len(infile) == 0:
            self.tab = np.array([])
        else:
            if isinstance(infile, str):
                infile = [infile]
            specialprint("Attempting read stips...")
            self.tab = WingTips.read_stips(infile[0], **kwargs)
            specialprint("After read_stips %f MB" % (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024))
            if len(infile) > 1:
                for i in range(1, len(infile)):
                    _tab = WingTips.read_stips(infile[i], **kwargs)
                    self.tab = np.vstack((self.tab, _tab))
            center = WingTips.get_center(self.tab[:, 0], self.tab[:, 1])
        self.center = center
        self.n = self.tab.shape[0]
        self.infile = infile
        gc.collect()

    ''' Strip coordinates from WingTips object '''

    def strip_radec(self, hasID=False):
        _i = int(hasID)
        self.tab = np.delete(self.tab, [_i, _i + 1], 1)
        return None

    ''' Attach given RA-DEC to WingTips object'''

    def attach_radec(self, radec, hasID=False):
        if self.n != radec.shape[0]:
            raise ValueError('Number of RA-DEC does not match sources')
        _i = int(hasID)
        self.tab = np.insert(self.tab, _i, radec.T, 1)
        self.center = WingTips.get_center(radec[:, 0 + _i], radec[:, 1 + _i])
        return None

    ''' Replace RA-DEC of WingTips object '''

    def replace_radec(self, radec, hasID=False):
        self.strip_radec(hasID)
        self.attach_radec(radec, hasID)
        return None

    ''' 
    Return random RA-DEC for given image or WingTips object
    Optionally, specify center and image size desired
    '''

    def random_radec_for(self, other, shape=(4096, 4096), sample=False, n=0, hasID=False):
        _i = int(hasID)
        # try:
        #    if other.endswith('.fits'):
        #        return WingTips.random_radec(self.n,imfile=other)
        # except AttributeError:
        if not sample:
            return WingTips.random_radec(self.n, center=other.center, shape=shape)
        elif not bool(n):
            return WingTips.sample_radec(n=self.n, radec1=False, radec2=other.tab[:, _i:_i + 1])
        else:
            return WingTips.sample_radec(n=n, radec1=self.tab[:, _i:_i + 1], radec2=other.tab[:, _i:_i + 1])

    ''' Merge two WingTips objects '''

    def merge_with(self, other, hasRADEC=True, hasID=False):
        if self.tab.shape[1] != other.tab.shape[1]:
            raise ValueError('Number of columns does not match', self.tab.shape[1], other.tab.shape[1])
        self.tab = np.vstack((self.tab, other.tab))
        self.n = self.tab.shape[0]
        self.infile.append(other.infile)
        _i = int(hasID)
        if hasRADEC:
            self.center = WingTips.get_center(self.tab[:, 0 + _i], self.tab[:, 1 + _i])
        return None

    ''' Convert flux to surface brightness for sersic profile galaxies '''

    def flux_to_Sb(self, hasRADEC=True, hasID=False):
        _i = int(hasID)
        if hasRADEC:
            _i = _i + 2
        _f = self.tab[:, _i].astype(float)
        _r = self.tab[:, _i + 3].astype(float)
        _a = self.tab[:, _i + 5].astype(float)
        _s = (0.5 * _f) / (np.pi * _r ** 2 * _a)
        self.tab = np.delete(self.tab, _i, 1)
        self.tab = np.insert(self.tab, _i, _s.T, 1)
        return None

    def write_stips(self, outfile='temp.txt', hasID=False, hasCmnt=False, saveID=False, ipac=False,
                    max_writing_packet=np.inf):
        """
        Write out a STIPS input file
        """
        #should change so that STIPS input file is fits rather than ascii
        gc.collect()
        _tab = WingTips.get_tabular(self.tab, hasID, hasCmnt, saveID)
        specialprint("After get_tabular %f MB" % (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024))
        _nms = ('id', 'ra', 'dec', 'flux', 'type', 'n', 're', 'phi', 'ratio', 'notes')
        _fmt = ('%10d', '%15.7f', '%15.7f', '%15.7f', '%8s', '%10.3f', '%15.7f', '%15.7f', '%15.7f', '%8s')
        _t = Table(_tab, names=_nms)
        del _tab
        gc.collect()
        specialprint("After astropy.Table %f MB" % (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024))
        _length = len(_t)
        _max = min(max_writing_packet, _length)
        outfile = open(outfile, 'w')
        for i in range(int(np.ceil(_length / _max))):
            ascii.write(_t[i * _max:(i + 1) * _max], outfile,
                        format=['fixed_width_no_header', ['fixed_width', 'ipac'][ipac]][i == 0],
                        formats=dict(zip(_nms, _fmt)),
                        **[{'delimiter': ' ', 'delimiter_pad': ''}, {}][(ipac and i == 0)])
        outfile.close()
        del _t
        gc.collect()
        specialprint("After ascii.write %f MB" % (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024))
        return specialprint('Wrote out %s \n' % outfile)

    def append_stips(self, outfile='temp.txt', hasID=False, hasCmnt=False, saveID=False, startID=1, ipac=False,
                    max_writing_packet=np.inf):
        """
        Append a STIPS input file
        """
        gc.collect()
        _tab = WingTips.get_tabular(self.tab, hasID, hasCmnt, saveID, startID)
        specialprint("After get_tabular %f MB" % (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024))
        #_nms = ('id', 'ra', 'dec', 'flux', 'type', 'n', 're', 'phi', 'ratio', 'notes')
        _nms = ('0', '0.0', '0.00', '0.000', 'point', '1.000', '1.0000', '1.00000', '1.0000000',  'comment')
        _fmt = ('%10d', '%15.7f', '%15.7f', '%15.7f', '%8s', '%10.3f', '%15.7f', '%15.7f', '%15.7f', '%8s')
        _t = Table(_tab, names=_nms)
        del _tab
        gc.collect()
        specialprint("After astropy.Table in append %f MB" % (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024))
        _length = len(_t)
        _max = min(max_writing_packet, _length)
        outname = str(outfile)
        outfile = open(outfile, 'a')
        for i in range(int(np.ceil(_length / _max))):
            ascii.write(_t[i * _max:(i + 1) * _max], outfile,
                        format=['fixed_width_no_header', ['fixed_width', 'ipac'][ipac]][i == 0],
                        formats=dict(zip(_nms, _fmt)),
                        **[{'delimiter': ' ', 'delimiter_pad': ''}, {}][(ipac and i == 0)])
        outfile.close()
        del _t
        gc.collect()
        specialprint("After ascii.write in append %f MB" % (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024))
        rmline = str(startID+1)
        specialprint(rmline)
        command = "sed -i \'"+rmline+"d\' "+outname
        _p=subprocess.run(command,shell=True)

        return specialprint('Appended %s \n' % outfile)

    @staticmethod
    def from_scratch(flux, ra=[], dec=[], center=[], ID=[], Type=[], n=[], re=[], phi=[], ratio=[], notes=[],
                     outfile=None, max_writing_packet=np.inf):
        """
        Build a WingTips class object from scratch
        """
        gc.collect()
        specialprint("Starting memory %f MB" % (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024))
        _temp = WingTips()
        _temp.n = len(flux)
        _temp.infile = ['fromScratch']
        #
        if len(center) > 0:
            _temp.center = center
            if len(ra) == 0:
                radec = _temp.random_radec_for(_temp)
                ra, dec = radec[:, 0], radec[:, 1]
        elif (len(ra) == len(dec)) & (len(ra) > 0):
            _temp.center = WingTips.get_center(np.array(ra), np.array(dec))
        else:
            raise ValueError('Provide valid coordinate or center')
        #
        if (len(Type) == 0) | (Type == 'point') | (Type == 'sersic'): #changed is for type to ==
            if (len(Type) == 0) | (Type == 'point'):
                Type = np.repeat(np.array(['point']), len(flux))
                _ones = np.ones_like(flux)
                n, re, phi, ratio = _ones, _ones, _ones, _ones
            elif Type == 'sersic':
                Type = np.repeat(np.array(['sersic']), len(flux))
        elif len(Type) == _temp.n:
            Type = np.array(Type)
        specialprint("After defining Type %f MB" % (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024))
        
        check = len(flux)

        if len(ID) == _temp.n:
            _tab = np.hstack((np.array(ID, ndmin=2).T, _tab))
            del ID
        if len(notes) == _temp.n:
            _tab = np.hstack((_tab, np.array(notes, ndmin=2).T))
            del notes
        
        if outfile is None:
            return _temp
        elif check < 10000000:
            _tab = np.array([ra, dec, flux, Type, n, re, phi, ratio], dtype='object').T
            _temp.tab = np.array(_tab)
            del _tab
            gc.collect()
            specialprint("After defining _temp.tab %f MB" % (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024))
            _temp.write_stips(outfile, hasID=bool(ID), hasCmnt=bool(notes), saveID=bool(ID),
                              max_writing_packet=max_writing_packet)
        elif check >= 10000000:
            runs = np.int(np.floor(check/10000000)+1)
            _tab = np.array([ra[0:10000000], dec[0:10000000], flux[0:10000000], Type[0:10000000], n[0:10000000], re[0:10000000], phi[0:10000000], ratio[0:10000000]], dtype='object').T
            _temp.tab = np.array(_tab)
            del _tab
            gc.collect()
            _temp.write_stips(outfile, hasID=bool(ID), hasCmnt=bool(notes), saveID=bool(ID),
                          max_writing_packet=max_writing_packet)
            for i in range(runs-1):
                index1 = (i+1)*10000000
                index2 = (i+2)*10000000 
                if index1 > check-1:
                   continue
                if index2 > check-1:
                   index2 = check-1
                specialprint("INDEXES %i %i" % (index1,index2))
                _tab = np.array([ra[index1:index2], dec[index1:index2], flux[index1:index2], Type[index1:index2], n[index1:index2], re[index1:index2], phi[index1:index2], ratio[index1:index2]], dtype='object').T
                #print(ra[index1:index2])
                #print(_tab)
                _temp.tab = np.array(_tab)
                del _tab
                gc.collect()
                _temp.append_stips(outfile, hasID=bool(ID), hasCmnt=bool(notes), saveID=bool(ID), startID=index1+1, max_writing_packet=max_writing_packet)
        del _temp
        gc.collect()
        specialprint("OUT OF LOOP")

    @staticmethod
    def read_stips(infile, getRADEC=True, getID=False, getCmnt=False, **kwargs):
        """
        Read in a STIPS input file in ascii format and
        return corresponding NumPy array
        """
        #most likely need to change to read in a STIPS input file in fits format
        gc.collect()
        include_names = getID * ['id'] + \
                        getRADEC * ['ra', 'dec'] + \
                        ['flux', 'type', 'n', 're', 'phi', 'ratio'] + \
                        getCmnt * ['comment']
        kwargs['usecols'] = include_names
        _temp = dd.read_table(infile, sep='\s+', **kwargs).to_dask_array().compute()
        specialprint("After dd.read_table > to_array > compute %f MB" % (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024))
        return _temp

    @staticmethod
    def get_tabular(_tab, hasID=False, hasCmnt=False, saveID=False, startID=1):
        """
        Return tabular lists for STIPS input file columns
        """
        _i = int(hasID)
        if ~saveID:
            _n = _tab.shape[0]
            #_ID = np.array(np.linspace(1, _n, _n), ndmin=2).T
            _ID = np.array(np.linspace(startID, startID-1+_n, _n), ndmin=2).T
            _tab = np.hstack((_ID, _tab[:, _i:]))
            del _ID
            specialprint("After ~saveID %f MB" % (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024))
        if ~hasCmnt:
            _cmnt = np.array(np.repeat(np.array(['comment']), _tab.shape[0], ), ndmin=2).T
            _tab = np.hstack((_tab, _cmnt))
            del _cmnt
            specialprint("After ~hasCmnt %f MB" % (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024))
        gc.collect()
        return [_tab[:, 0].astype(float), _tab[:, 1].astype(float), _tab[:, 2].astype(float),
                _tab[:, 3].astype(float), _tab[:, 4], _tab[:, 5].astype(float),
                _tab[:, 6].astype(float), _tab[:, 7].astype(float),
                _tab[:, 8].astype(float), _tab[:, 9]]

    ''' Build WCS coordinate system from scratch '''

    @staticmethod
    def create_wcs(centers=[0, 0], crpix=[2048, 2048], cdelt=[-0.11 / 3600, 0.11 / 3600], cunit=['deg', 'deg'], \
                   ctype=['RA---TAN', 'DEC--TAN'], lonpole=180, latpole=24.333335, \
                   equinox=2000.0, radesys='ICRS'):
        _w = wcs.WCS()
        _w.wcs.cdelt = cdelt
        _w.wcs.crpix = crpix
        _w.wcs.crval = centers
        _w.wcs.cunit = cunit
        _w.wcs.ctype = ctype
        _w.wcs.lonpole = lonpole
        _w.wcs.latpole = latpole
        _w.wcs.radesys = radesys
        _w.wcs.equinox = equinox
        return _w

    ''' Return coordinate system for given image file'''

    @staticmethod
    def read_wcs(imfile):
        specialprint('Getting coordinates from %s \n' % imfile)
        return wcs.WCS(fits.open(imfile)[1].header)

    ''' Return 'n' random radec for given image file or coordinate list '''

    @staticmethod
    def random_radec(n=10, center=[0, 0], shape=(4096, 4096), imfile=''):
        _xy = np.random.rand(n, 2) * shape
        if imfile != '': #changed is not to !=
            _w = WingTips.read_wcs(imfile)
        else:
            _w = WingTips.create_wcs(center)
        return _w.wcs_pix2world(_xy, 1)

    '''
    Return a random sample of 'n' RA-DEC coordinates from 'radec2'
    If radec1 is specified, then replace 'n' radom coordinates
    in 'radec1' with random sample from 'radec2'
    '''

    @staticmethod
    def sample_radec(n=10, radec1=False, radec2=[]):
        in2 = np.random.randint(0, radec2.shape[0], n)
        if ~radec1:
            return radec2[in2, :]
        else:
            in1 = np.random.randint(0, radec1.shape[0], n)
            radec1[in1, :] = radec2[in2, :]
            return radec1

    ''' Return mean of RA-DEC positions given '''

    @staticmethod
    def get_center(ra, dec):
        return [ra.astype(float).mean(), dec.astype(float).mean()]

    '''
    Convert mags to WFI instrument counts
    Default is apparent AB mags
    Specify 'dist' if absolute mags
    Specify AB_Vega if Vega mags
    '''

    @staticmethod
    def get_counts(mag, ZP, dist=0, AB_Vega=0):
        if bool(dist):
            specialprint('\nDistance is d = %4.2f Mpc\n' % dist)
            u = 25 + 5 * np.log10(dist)
            mag = mag + u
        if bool(AB_Vega):
            mag = mag + AB_Vega
        return 10 ** ((mag - ZP) / (-2.5))
