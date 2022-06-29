#! /usr/bin/env python
import wpipe as wp


def register(task):
    _temp = task.mask(source='*', name='start', value=task.name)
    _temp = task.mask(source='*', name='images_prepped', value='*')


def write_dolphot_pars(target, config, thisjob, detname):
    #parfile_name = target.name + "_" + detname + ".param"
    parfile_name = detname + ".param"
    parfile_path = config.confpath + '/' + parfile_name
    thisjob.logprint(''.join(["Writing dolphot pars now in ", parfile_path, "\n"]))
    #my_dp = config.dataproducts
    #datadp = my_dp[my_dp.subtype == 'dolphot_data']
    my_dp = [ _temp for _temp in config.dataproducts]
    datadp = [ _temp for _temp in config.dataproducts if _temp.subtype == 'dolphot_data']
    datadpid = [_dp.dp_id for _dp in datadp]
    dataname = [_dp.filename for _dp in datadp]
    print("DATANAME ",dataname)
    rinds = []
    zinds = []
    yinds = []
    jinds = []
    hinds = []
    finds = []
    count = 0
    for dp in datadp:
        dp_id = dp.dp_id
        filt = str(dp.filtername)
        fname = dp.filename
        if "F062" in filt and detname in fname:
            rinds = [rinds, dp_id]
            count += 1
        if "F087" in filt and detname in fname:
            zinds = [zinds, dp_id]
            count += 1
        if "F106" in filt and detname in fname:
            yinds = [yinds, dp_id]
            count += 1
        if "F129" in filt and detname in fname:
            jinds = [jinds, dp_id]
            count += 1
        if "F158" in filt and detname in fname:
            hinds = [hinds, dp_id]
            count += 1
        if "F184" in filt and detname in fname:
            finds = [finds, dp_id]
            count += 1
    rinds = rinds[1:]
    zinds = zinds[1:]
    yinds = yinds[1:]
    jinds = jinds[1:]
    hinds = hinds[1:]
    finds = finds[1:]

    print("INDS ", rinds, zinds, yinds, jinds, hinds, hinds, finds, datadpid)
    nimg = count
    # my_params = config.parameters
    # refimage = my_params['refimage']  #will make this more flexible later
    refdp = wp.DataProduct(hinds[0])
    refimage = str(refdp.filename)
    if "sim" in refimage:
       refimage = target.name + '_' + detname + '_' + str(refdp.dp_id) + '_' + refdp.filtername + ".fits"
    else:
       print("No sim")
    print(refimage)
    with open(parfile_path, 'w') as d:
        d.write("Nimg = " + str(nimg) + "\n" +
                "img0_file = " + refimage[:-5] + "\n")
        rim = []
        for rind in rinds:
            imdp = wp.DataProduct(rind)
            image = str(imdp.filename)
            if 'sim' in image:
               image = target.name + '_' + detname + '_' + str(imdp.dp_id) + '_' + imdp.filtername + ".fits"
            rim = [rim, image]
        rim = rim[1:]
        zim = []
        for zind in zinds:
            imdp = wp.DataProduct(zind)
            image = str(imdp.filename)
            zim = [zim, image]
        zim = zim[1:]
        yim = []
        for yind in yinds:
            imdp = wp.DataProduct(yind)
            image = str(imdp.filename)
            yim = [yim, image]
        yim = yim[1:]
        jim = []
        for jind in jinds:
            imdp = wp.DataProduct(jind)
            image = str(imdp.filename)
            jim = [jim, image]
        jim = jim[1:]
        him = []
        for hind in hinds:
            imdp = wp.DataProduct(hind)
            image = str(imdp.filename)
            him = [him, image]
        him = him[1:]
        fim = []
        for find in finds:
            fim = []
            imdp = wp.DataProduct(find)
            image = str(imdp.filename)
            fim = [fim, image]
        fim = fim[1:]
        # zims = set(zim)
        # yims = set(yim)
        # jims = set(jim)
        # hims = set(him)
        # fims = set(fim)
        images = [rim,zim, yim, jim, him, fim]
        i = 0
        for iimage in images:
            image = iimage[0]
            print("IMAGE ", image)
            i += 1
            d.write("img" + str(i) + "_file = " + image[:-5] + "\n")
        d.write("img_shift = 0 0\n" +
                "img_xform = 1 0 0\n" +
                "img_RAper = 5\n" +
                "img_RChi  = 2\n" +
                "img_RSky  = 15 35\n" +
                "img_RPSF  = 13\n" +
                "img_apsky = 15 35\n" +

                "RCentroid = 1           #centroid box size (int>0)\n" +
                "SigFind = 3.0           #sigma detection threshold (flt)\n" +
                "SigFindMult = 0.85      #Multiple for quick-and-dirty photometry (flt>0)\n" +
                "SigFinal = 3.5          #sigma output threshold (flt)\n" +
                "MaxIT = 25              #maximum iterations (int>0)\n" +
                "PSFPhot = 1             #photometry type (int/0=aper,1=psf,2=wtd-psf)\n" +
                "PSFPhotIt = 1           #number of iterations in PSF-fitting photometry (int>=0)\n" +
                "FitSky = 2              #fit sky? (int/0=no,1=yes,2=small,3=with-phot)\n" +
                "SkipSky = 2             #spacing for sky measurement (int>0)\n" +
                "SkySig = 2.25           #sigma clipping for sky (flt>=1)\n" +
                "NegSky = 1              #allow negative sky values? (0=no,1=yes)\n" +
                "NoiseMult = 0.1        #noise multiple in imgadd (flt)\n" +
                "FSat = 0.999            #fraction of saturate limit (flt)\n" +
                "PosStep = 0.1           #search step for position iterations (flt)\n" +
                "dPosMax = 2.5           #maximum single-step in position iterations (flt)\n" +
                "RCombine = 1.5          #minimum separation for two stars for cleaning (flt)\n" +
                "SigPSF = 10             #min S/N for psf parameter fits (flt)\n" +
                "UseWCS = 2              #use WCS info in alignment (int 0=no, 1=shift/rotate/scale, 2=full)\n" +
                "Align = 3               #align images? (int 0=no,1=const,2=lin,3=cube)\n" +
                "AlignOnly = 0           #exit after alignment\n" +
                "SubResRef = 1           #subpixel resolution for reference image (int>0)\n" +
                "SecondPass = 3          #second pass finding stars (int 0=no,1=yes)\n" +
                "SearchMode = 1          #algorithm for astrometry (0=max SNR/chi, 1=max SNR)\n" +
                "Force1 = 0              #force type 1/2 (stars)? (int 0=no,1=yes)\n" +
                "PSFres = 0              #make PSF residual image? (int 0=no,1=yes)\n" +
                "ApCor = 1               #find/make aperture corrections? (int 0=no,1=yes)\n" +
                "FakeStars =             #file with fake star input data\n" +
                "FakeOut =               #file with fake star output data (default=phot.fake)\n" +
                "FakeMatch = 3.0         #maximum separation between input and recovered star (flt>0)\n" +
                "FakePSF = 2.0           #assumed PSF FWHM for fake star matching\n" +
                "FakeStarPSF = 1         #use PSF residuals in fake star tests? (int 0=no,1=yes)\n" +
                "RandomFake = 1          #apply Poisson noise to fake stars? (int 0=no,1=yes)\n" +
                "FakePad = 0             #minimum distance of fake star from any chip edge to be used\n" +
                "DiagPlotType = PS       #format to generate diagnostic plots (PNG, GIF, PS)\n" +
                "VerboseData = 1         #to write all displayed numbers to a .data file\n" +
                "ForceSameMag = 0        #force same count rate in images with same filter? (int 0=no, 1=yes)\n" +
                "FlagMask = 4            #photometry quality flags to reject when combining magnitudes\n" +
                "CombineChi = 0          #combined magnitude weights uses chi? (int 0=no, 1=yes)\n" +
                "InterpPSFlib = 0        #interpolate PSF library spatially\n")
    _dp = config.dataproduct(filename=parfile_name, relativepath=config.confpath,
                             subtype="dolphot_parameters", group='conf')
    return _dp


def parse_all():
    parser = wp.PARSER
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_all()
    this_job_id = args.job_id
    this_job = wp.Job(this_job_id)
    this_event = this_job.firing_event
    detname = this_event.options['detname']
    this_event_id = this_event.event_id
    this_config = this_job.config
    this_target = this_config.target
    tid = this_target.target_id
    this_job.logprint(''.join(["detname ", str(detname), "\n"]))
    paramdp = write_dolphot_pars(this_target, this_config, this_job, detname)
    dpid = int(paramdp.dp_id)
    this_job.logprint(''.join(["Parameter file DPID ", str(dpid), "\n"]))
    newevent = this_job.child_event('parameters_written', tag=dpid, options={'target_id': tid, 'dp_id': dpid, 'detname': detname,'submission_type': 'scheduler'})
    newevent.fire()
    this_job.logprint('parameters_written\n')
