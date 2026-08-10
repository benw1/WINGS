#! /usr/bin/env python
import time
import wpipe as wp
import numpy as np
from astropy.io import fits
from astropy.wcs import WCS
import astropy.coordinates as ac
import astropy.table as at
from astropy.io import fits
from astropy.time import Time
from astropy.wcs import WCS
from astropy import units as u
from reproject import reproject_interp
from reproject.mosaicking import find_optimal_celestial_wcs, reproject_and_coadd
from romancal.associations import asn_from_list
from romancal.pipeline import MosaicPipeline
import asdf
import crds
import os
import warnings

def register(task):
    _temp = task.mask(source="*", name="start", value=task.name)
    _temp = task.mask(source="*", name="multi_dither_prepped", value="*")
    _temp = task.mask(source="*", name="isim_exposures_prepped", value="*")


def make_level3_image(file_list,outname):
    asn = asn_from_list.asn_from_list([(im, 'science') for im in l2_list],
                                      product_name=product_name,
                                      with_exptype=True,
                                      target='none')
    result = MosaicPipeline.call(asn,
                                 configure_log=False,
                                 on_disk=True,
                                 save_results=True,
                                 steps={'skymatch':{'skip': True},
                                        'outlier_detection':{'skip':True},
                                        'source_catalog':{'skip':True}})
    level3_path = outname+"_coadd.asdf"
    return level3_path

def asdf_to_fits(im, json_file):
    '''Convert Roman L2 image datamodel asdf format to FITS.
    
    Inputs
    ------
    im : roman datamodel
        Roman L2 image or romanisim output
    json_file : path-like
        Path to JSON file with mapping of metadata to FITS keywords.
        
    Returns
    -------
    hdulist : fits.HDUList
        FITS HDUList in dolphot-ready format (equivalent to romanmask output).
    '''
    # asdf to fits keywords
    with open(json_file, 'r') as f:
        key_map = json.load(f) 
    ny, nx = im.data.shape
    sip_header = im.meta.wcs.to_fits_sip(bounding_box=((-0.5, nx - 0.5), (-0.5, ny - 0.5)))
    img_hdu = fits.PrimaryHDU(header=sip_header, data=im.data)
    img_hdu.header.set('EXTNAME', 'DATA')
    img_hdu.header.set('BUNIT', 'DN', 'Image units')
    pri_hdu = img_hdu
    # pri_hdu = fits.PrimaryHDU()
    # relevant metadata
    for key in key_map.keys():
        if hasattr(im.meta, key):
            pri_hdu.header = update_fits_header_from_meta(key_map[key], pri_hdu.header, getattr(im.meta, key))
    # JANKY
    if hasattr(im.meta, 'ref_file'):
        if 'crds://' in im.meta.ref_file.gain:
            gainfile = os.path.join(os.environ['CRDS_PATH'], 'references/roman/wfi', 
                                    im.meta.ref_file.gain.split('crds://')[-1])
            with rdm.open(gainfile) as gn:
                # gn_mean = np.nanmean(gn.data[4:-4, 4:-4])
                # img_hdu.data *= gn.data[4:-4, 4:-4] / gn_mean
                img_hdu.header.set('GAIN', np.nanmean(gn.data[4:-4, 4:-4]))
        else:
            img_hdu.header.set('GAIN', rparam.reference_data['gain'])
        # img_hdu.header.set('GAIN', 1.0)
        if 'crds://' in im.meta.ref_file.readnoise:
            rnfile = os.path.join(os.environ['CRDS_PATH'], 'references/roman/wfi', 
                                im.meta.ref_file.readnoise.split('crds://')[-1])
            with rdm.open(rnfile) as rn:
                img_hdu.header.set('RDNOISE', np.nanmean(rn.data[4:-4, 4:-4]))
        else:
            img_hdu.header.set('RDNOISE', rparam.reference_data['readnoise'])
        
        crds_param = {'ROMAN.META.INSTRUMENT.DETECTOR': im.meta.instrument.detector,
                    'ROMAN.META.INSTRUMENT.NAME': im.meta.instrument.name,
                    'ROMAN.META.INSTRUMENT.OPTICAL_ELEMENT' : im.meta.instrument.optical_element,
                    'ROMAN.META.EXPOSURE.TYPE' : im.meta.exposure.type,
                    'ROMAN.META.EXPOSURE.START_TIME': im.meta.exposure.start_time.isot}
        area_ref = None
        try:
            reffiles = crds.getreferences(crds_param, observatory='roman', 
                                        reftypes=['area'], # , 'readnoise', 'gain'
                                        context=im.meta.ref_file.crds.context,
                                        ignore_cache=False, fast=True)
            area_ref = reffiles['area']
        except Exception:
            print('Failed to acquire reference file(s).')
        if area_ref is not None:
            pamfile = rdm.open(area_ref)
            pam = pamfile.data
        else:
            pam = calc_pix_area(WCS(img_hdu.header))
        img_hdu.data *= pam * img_hdu.header['EFFTIME']
    if hasattr(im, 'dq'):
        mask_sat = (im.dq & 2) > 0
        mask_bad = (im.dq & 1+8+1024) > 0
        bad_val = min(img_hdu.data[~(mask_bad | mask_sat)].min() * 1.1, -100.)
        sat_val = max(img_hdu.data[~(mask_sat | mask_bad)].max() * 1.1, 65536.)
        img_hdu.data[mask_bad] = bad_val
        img_hdu.data[mask_sat] = sat_val
        pri_hdu.header.set('BADPIX', bad_val)
        pri_hdu.header.set('SATURATE', sat_val)
        pri_hdu.header.set('MJD-OBS', pri_hdu.header['MID_TIME'])
        pri_hdu.header.set('AIRMASS', 0.0)
        pri_hdu.header.set('EXPTIME0', pri_hdu.header['EFFTIME'])
    if ('PHOTMJSR' in pri_hdu.header.keys()) and ('PIXAREA' in pri_hdu.header.keys()):
        cps_to_mjy = pri_hdu.header['PHOTMJSR'] * pri_hdu.header['PIXAREA'] * 1e6
        pri_hdu.header.set('DOL_C2JY', -2.5 * np.log10(cps_to_mjy))
    else:
        pri_hdu.header.set('DOL_C2JY', 0)
    pri_hdu.header.set('DOL_ROMN', 0)
    hdulist = fits.HDUList([pri_hdu])
    return hdulist

def combine_average_fits_images(file_list):
    """
    Combines and averages multiple FITS images with different WCS into a single mosaic.

    The method finds an optimal output WCS that encompasses all input images,
    reprojects each image onto that common grid, and then calculates the
    average pixel value for overlapping regions.

    Args:
        file_list (list): A list of paths to the FITS files to combine.

    Returns:
        tuple: A tuple containing:
            - numpy.ndarray: The combined and averaged image data.
            - astropy.io.fits.Header: The header for the combined image (containing the output WCS).
    """
    if not file_list:
        raise ValueError("The input file list cannot be empty.")

    # 1. Load images and determine the optimal output WCS and shape
    # The find_optimal_celestial_wcs function takes a list of HDU objects, filenames, or (array, wcs) tuples.
    input_hdus = [fits.open(f)[1] for f in file_list]
    wcs_out, shape_out = find_optimal_celestial_wcs(input_hdus, auto_rotate=True, frame='fk5')

    # Create an empty array to store the combined data (summed values) and footprint (count)
    # The reproject_and_coadd function handles the accumulation.
    combined_data, combined_footprint = reproject_and_coadd(
        input_hdus,
        wcs_out,
        shape_out=shape_out,
        reproject_function=reproject_interp, # Use interpolation for speed and generality
        match_background=False # Set to True if backgrounds need matching
    )
    
    # 2. Convert the summed data to an average
    # The combined_data array contains the sum of pixel values where images overlap.
    # The combined_footprint array contains the number of images that contributed to each pixel.
    # Divide sum by count to get the average, handle potential division by zero (where footprint is 0)
    # The footprint is typically a float array (with values like 0.0 or 1.0) but can have fractional values 
    # depending on the reprojection method. 
    # The default behavior of reproject_and_coadd is to sum the values.
    
    # Use numpy.divide to safely perform division, setting invalid values (0 footprint) to NaN or 0
    averaged_data = np.divide(combined_data, combined_footprint, 
                              out=np.zeros_like(combined_data), where=combined_footprint!=0)
    
    # 3. Create the output header
    header_out = wcs_out.to_header()
    header_out.insert(0,('NAXIS2',shape_out[0]))
    header_out.insert(0,('NAXIS1',shape_out[1]))
    header_out.insert(0,('NAXIS',2))
    header_out.insert(0,('BITPIX',32))
    header_out.insert(0,('SIMPLE','T'))

    header_out['COMMENT'] = 'Combined and averaged FITS images using astropy/reproject.'
    
    print(header_out)
    
    # Close the opened FITS files to free resources
    for hdu in input_hdus:
        hdu.fileinfo()['file'].close()

    return combined_data, header_out


def parse_all():
    parser = wp.PARSER
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_all()
    this_job_id = args.job_id
    this_job = wp.Job(this_job_id)
    this_event = this_job.firing_event
    detname = this_event.options['detname']
    chip = this_event.options['chip']
    chipname = "chip"+str(chip)+"."
    this_job.logprint("DETNAME AND CHIP")
    this_job.logprint(detname)
    this_job.logprint(chip)
    this_event_id = this_event.event_id
    this_config = this_job.config
    this_target = this_config.target
    datadp = wp.DataProduct.select(config_id=str(this_config.config_id), subtype=detname)
    datadpid = [_dp.dp_id for _dp in datadp]
    dataname = [_dp.filename for _dp in datadp]
    print("DATANAME ",dataname)
    this_job.logprint(''.join(["detname ", str(detname), "\n"]))
    rinds = []
    zinds = []
    yinds = []
    jinds = []
    hinds = []
    finds = []
    kinds = []
    count = 0
    for dp in datadp:
        dp_id = dp.dp_id
        filt = str(dp.filtername)
        fname = dp.filename
        print("FNAME: ",fname)
        if chipname in fname:
           this_job.logprint(''.join([chipname, " in ",fname,"\n"]))
           continue
        #print('fname = ', fname)
        if "F158" in filt and detname in fname:
            hinds.append(dp_id)
            count += 1
            print('hinds = ', hinds)
            him = []
    for hind in hinds:
        imdp = wp.DataProduct(hind)
        image = str(imdp.relativepath) + "/" + str(imdp.filename)
        him.append(image)

    tid = this_target.target_id
    fits_files = him
    this_job.logprint(f"FITS FILES ARE {fits_files}")

    if ("dither" in this_event.name):  
        output_filename =  str(detname)+"_reference.fits"
        output_filepath = this_config.procpath + "/" + str(detname)+"_reference.fits"
        combined_data_avg, combined_header = combine_average_fits_images(fits_files)
        fits.writeto(output_filepath, combined_data_avg, header=combined_header, overwrite=True)
        this_job.logprint(f"Successfully combined images and saved to {output_filepath}")
        reference_dp = this_config.dataproduct(filename=output_filename, relativepath=this_config.procpath,
                             subtype="reference_image", group='proc')
    else:
        output_filename =  str(detname)+"_coadd.fits"
        output_filepath =  this_config.procpath + "/" + str(detname)+"_coadd.fits"
        output_fitsname =  str(detname)+"_reference.fits"
        output_fitspath =  this_config.procpath + "/" + str(detname)+"_reference.fits"
        level3_outname = this_config.procpath + "/" + str(detname)
        level3_path = make_level3_image(fits_files,level3_outname)
        fits_hdu = asdf_to_fits(level3_path, this_config.parameters['asdf_to_fits_json'])
        fits_hdu.writeto(output_fitspath, overwrite=True)
        
        this_job.logprint(f"Successfully combined images and saved to {level3_path}")
        reference_dp = this_config.dataproduct(filename=output_fitsname, relativepath=this_config.procpath,
                             subtype="reference_image", group='proc')


    dpid = int(reference_dp.dp_id)
    this_job.logprint(''.join(["Reference file DPID ", str(dpid), "\n"]))
    newevent = this_job.child_event('reference_prepped', tag=dpid, options={'target_id': tid, 'dp_id': dpid, 'detname': detname,'submission_type': 'scheduler', 'chip': this_event.options['chip']})
    newevent.fire()
    this_job.logprint('reference_prepped\n')
    time.sleep(300)

        
