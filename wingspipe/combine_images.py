#! /usr/bin/env python
import time
import wpipe as wp
import numpy as np
from astropy.io import fits
from astropy.wcs import WCS
from reproject import reproject_interp
from reproject.mosaicking import find_optimal_celestial_wcs, reproject_and_coadd
import warnings

def register(task):
    _temp = task.mask(source="*", name="start", value=task.name)
    _temp = task.mask(source="*", name="multi_dither_prepped", value="*")


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

    combined_data_avg, combined_header = combine_average_fits_images(fits_files)

    output_filepath = this_config.procpath + "/" + str(detname)+"_reference.fits"
    output_filename =  str(detname)+"_reference.fits"
    fits.writeto(output_filepath, combined_data_avg, header=combined_header, overwrite=True)
    this_job.logprint(f"Successfully combined images and saved to {output_filepath}")
    reference_dp = this_config.dataproduct(filename=output_filename, relativepath=this_config.procpath,
                             subtype="reference_image", group='proc')

    dpid = int(reference_dp.dp_id)
    this_job.logprint(''.join(["Reference file DPID ", str(dpid), "\n"]))
    newevent = this_job.child_event('reference_prepped', tag=dpid, options={'target_id': tid, 'dp_id': dpid, 'detname': detname,'submission_type': 'scheduler', 'chip': this_event.options['chip']})
    newevent.fire()
    this_job.logprint('reference_prepped\n')
    time.sleep(300)

        
