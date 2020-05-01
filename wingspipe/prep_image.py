#! /usr/bin/env python
import glob
import subprocess
from shutil import which

from astropy.io import fits
import wpipe as wp


def register(task):
    _temp = task.mask(source='*', name='start', value=task.name)
    _temp = task.mask(source='*', name='stips_done', value='*')


def outimages(imgpath):
    front = imgpath.split('.fits')[0]
    print("FRONT: ", front)
    chips = glob.glob(front + '.chip*.fits')
    print("CHIPS: ", chips, ". \n")
    return chips


def prep_images(config):
    my_dp = config.dataproducts
    imagedp = my_dp[my_dp['subtype'] == 'stips_image']
    _job = wp.Job(config=config).create()  # need to create dummy job to keep track of events
    my_job = config.job()
    target = config.target
    targ = target.name
    print("TARGET NAME: ", targ, "\n")
    comp_name = 'completed' + targ
    options = {comp_name: 0}
    my_job.options = options
    for dp_id in imagedp.dp_id:
        print("DP_ID ", str(dp_id), " sent\n")
        my_job.logprint(''.join(["DP_ID ", str(dp_id), " sent\n"]))
        send(dp_id, config, comp_name, len(imagedp), my_job)  # send catalog to next step
    return


def send(dpid, conf, comp_name, total, job):
    dp = wp.DataProduct(int(dpid))
    target_id = conf.target_id
    filepath = dp.relativepath + '/' + dp.filename
    event = job.child_event('stips_done', tag=dpid,
                            options={'dp_id': dpid, 'target_id': target_id, 'to_run': total, 'name': comp_name})
    job.logprint(''.join(["Firing stips_done for ", str(filepath), " one of ", str(total), "\n"]))
    event.fire()
    return


def prep_image(imgpath, filtname, config, thisjob, dp_id):
    thisjob.logprint(''.join(['running ', imgpath, ' in filter ', filtname]))
    print("GOT DP ", str(dp_id))
    my_params = config.parameters
    dolphot_path = which('wfirstmask')
    dolphot_path = dolphot_path[:-10]
    target = config.target
    targetname = target.name
    print(targetname, " TARGET\n")
    new_image_name = targetname + '_' + filtname + ".fits"
    imgpath = config.procpath + '/' + new_image_name
    # CHANGE FILENAME IN DATA PRODUCT.  ASK RUBAB
    dp = wp.DataProduct(int(dp_id))
    dp.filename = new_image_name
    # fixwcs(imagepath)
    _t1 = [dolphot_path + 'wfirstmask', '-exptime=' + str(my_params['exptime']), '-rdnoise=41.73', imgpath]
    _t2 = [dolphot_path + 'splitgroups', imgpath]
    print("T1 ", _t1)
    print("T2 ", _t2)
    _t = subprocess.run(_t1, stdout=subprocess.PIPE)
    _t = subprocess.run(_t2, stdout=subprocess.PIPE)
    outims = outimages(imgpath)
    if len(outims) > 1:
        print(len(outims), " Images\n")
        # for outimage in outimages:
        # placeholder for when there are 18 chips in each sim
    else:
        filename = outims[0].split('/')[-1]
        front = filename.split('.fits')[0]
        _t3 = [dolphot_path + 'calcsky', config.procpath + '/' + front, '15', '35', '-64', '2.25',
               '2.00']  # put in calcsky parameters
        print("T3 ", _t3)
        _t = subprocess.run(_t3, stdout=subprocess.PIPE)
        _dp1 = config.dataproduct(filename=filename, relativepath=config.procpath, group='proc',
                                  subtype='dolphot_data', filtername=filtname)
        skyname = front + '.sky.fits'
        _dp2 = config.dataproduct(filename=skyname, relativepath=config.procpath, group='proc',
                                  subtype='dolphot_sky', filtername=filtname)


def fixwcs(imgpath):
    # use astropy to get CDELT1 and CDELT2 from header
    data, head = fits.getdata(imgpath, header=True)
    cd11 = head['PC1_1']
    cd22 = head['PC2_2']
    fits.setval(imgpath, 'CD1_1', value=cd11)
    fits.setval(imgpath, 'CD2_2', value=cd22)
    fits.setval(imgpath, 'CD1_2', value=0)
    fits.setval(imgpath, 'CD2_1', value=0)
    return


def parse_all():
    parser = wp.PARSER
    parser.add_argument('--job', '-j', type=int, dest='job_id',
                        help='This job ID')
    parser.add_argument('--config', '-c', type=int, dest='config_id',
                        help='Configuration ID')
    parser.add_argument('--target', '-t', type=int, dest='target_id',
                        help='Target ID')

    return parser.parse_args()


if __name__ == '__main__':
    args = parse_all()
    if args.config_id:
        this_config = wp.Configuration(args.config_id)
        prep_images(this_config)
    elif args.target_id:
        this_target = wp.Target(args.target_id)
        all_conf = this_target.configurations
        for this_config in all_conf:
            print(this_config)
            prep_images(this_config)
    else:
        this_job_id = args.job_id
        this_job = wp.Job(this_job_id)
        this_event = this_job.firing_event
        this_event_id = this_event.event_id
        this_config = this_job.config
        print(this_config.config_id)
        this_dp_id = this_event.options['dp_id']
        tid = this_event.options['target_id']
        this_target = this_config.target
        this_dp = wp.DataProduct(this_dp_id)
        filtername = this_dp.filtername
        imagepath = this_dp.relativepath + '/' + this_dp.filename
        prep_image(imagepath, filtername, this_config, this_job, this_dp_id)
        parent_job_id = this_event.parent_job_id
        parent_job = this_event.parent_job
        compname = this_event.options['name']
        update_option = parent_job.options[compname]
        update_option = update_option + 1
        parent_job.options[compname] = update_option
        to_run = this_event.options['to_run']
        completed = update_option
        catalogID = this_event.options['dp_id']
        catalogDP = wp.DataProduct(catalogID)
        this_conf = catalogDP.config
        print(''.join(["Completed ", str(completed), " of ", str(to_run)]))
        this_job.logprint(''.join(["Completed ", str(completed), " of ", str(to_run), "\n"]))
        if completed >= to_run:
            new_event = this_job.child_event('images_prepped', options={'target_id': tid})
            new_event.fire()
            this_job.logprint('images_prepped\n')
            this_job.logprint(''.join(["Event= ", str(new_event.event_id), " images_prepped\n"]))
