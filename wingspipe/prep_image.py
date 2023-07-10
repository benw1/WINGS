#! /usr/bin/env python
import glob
import subprocess
from shutil import which
import time

from astropy.io import fits
import wpipe as wp


def register(task):
    _temp = task.mask(source='*', name='start', value=task.name)
    _temp = task.mask(source='*', name='stips_done', value='*')


def outimages(imgpath):
    front = imgpath.split('.fits')[0] +'.fits' + imgpath.split('.fits')[1]
    #issue with file name-- data/targetname/proc etc. targetname has .fits
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
    print('send filepath = ', filepath)
    event = job.child_event('stips_done', tag=dpid,
                            options={'dp_id': dpid, 'target_id': target_id, 'to_run': total, 'name': comp_name})
    job.logprint(''.join(["Firing stips_done for ", str(filepath), " one of ", str(total), "\n"]))
    event.fire()
    return


def prep_image(imgpath, filtname, config, thisjob, dp_id):
    thisjob.logprint(''.join(['running ', imgpath, ' in filter ', filtname]))
    print("GOT DP ", str(dp_id))
    dp = wp.DataProduct(int(dp_id))
    incatname = dp.subtype
    incatpre = incatname.split('.')[0]
    my_params = config.parameters
    dolphot_path = which('romanmask') 
    print(dolphot_path) 
    dolphot_path = dolphot_path[:-9] 
    target = config.target
    targetname = target.name
    print(targetname, " TARGET\n")
    #new_image_name = targetname + '_' + str(dp_id) + '_' + filtname + ".fits"
    #new_image_name = targetname + '_' + incatpre + '_' + str(dp_id) + '_' + filtname + ".fits" #why add target name? this doubles the '.fits'
    new_image_name = incatpre + '_' + str(dp_id) + '_' + filtname + ".fits"
    print("new image name = ", new_image_name)
    imgpath = config.procpath + '/' + new_image_name
    print('imgpath =', imgpath)
    outims = outimages(imgpath) #returns chips
    if len(outims) > 0: # purpose?
        return 0
    try:
        dp.filename = new_image_name
    except:
        print("name already changed")
    fixwcs(imgpath)
    _t1 = [dolphot_path + 'romanmask', '-exptime=' + str(my_params['exptime']), '-rdnoise=12.0', imgpath]
    _t2 = [dolphot_path + 'splitgroups', imgpath]
    print("T1 ", _t1)
    print("T2 ", _t2)
    _t = subprocess.run(_t1, stdout=subprocess.PIPE)
    _t = subprocess.run(_t2, stdout=subprocess.PIPE)
    outims = outimages(imgpath) #returns chips
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
    return 1


def fixwcs(imgpath):
    # use astropy to get CDELT1 and CDELT2 from header
    data, head = fits.getdata(imgpath, header=True)
    #cd11 = head['PC1_1']
    #cd22 = head['PC2_2']
    try:
        cd11 = head['CD1_1']
    except:
        cd11 = head['CDELT1']
        cd22 = head['CDELT2']
        fits.setval(imgpath, 'CD1_1', value=cd11)
        fits.setval(imgpath, 'CD2_2', value=cd22)
        fits.setval(imgpath, 'CD1_2', value=0)
        fits.setval(imgpath, 'CD2_1', value=0)
    return


def parse_all():
    parser = wp.PARSER
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
        numdet = int(this_config.parameters['ndetect'])

        this_dp_id = this_event.options['dp_id']
        tid = this_event.options['target_id']
        this_target = this_config.target
        this_dp = wp.DataProduct(this_dp_id)
        filtername = this_dp.filtername
        imagepath = this_dp.relativepath + '/' + this_dp.filename
        needcheck = prep_image(imagepath, filtername, this_config, this_job, this_dp_id)
        this_job.logprint(''.join(["Needcheck ", str(needcheck), "\n"]))
        parent_job_id = this_event.parent_job_id
        parent_job = this_event.parent_job
        compname = this_event.options['name']
        this_job.logprint(''.join(["Compname is ", str(compname), "\n"]))
        #########
        # wp.si.session.execute('LOCK TABLES options WRITE, optowners WRITE, jobs WRITE')
        # update_option = parent_job.options[compname]
        # update_option = update_option + 1
        # parent_job.options[compname] = update_option
        # wp.si.session.execute('UNLOCK TABLES')
        ######
        update_option = parent_job.options[compname]
        if (needcheck == 1):
            update_option += 1
        else:
            #update_option += 1
            this_job.logprint(''.join(["Needcheck not 1", str(needcheck), "\n"]))
        ######
        to_run = this_event.options['to_run']
        catalogID = this_event.options['dp_id']
        catalogDP = wp.DataProduct(catalogID)
        this_conf = catalogDP.config
        this_job.logprint(''.join(["Completed ", str(update_option), " of ", str(to_run), "\n"]))
        if update_option == to_run:
            '''
            this_job.logprint(''.join(["Detnames =",detnames1,"\n"]))
            for i in range(numdet):
                try: 
                    detname = this_event.options['detname'] 
                except:
                    detname = this_target.name
                print(detname)
         
                new_event = this_job.child_event('images_prepped', tag=detname, options={'target_id': tid,'detname': detname,'submission_type': 'pbs'})
                this_job.logprint('about to fire')
                new_event.fire()
                this_job.logprint('fired')
                this_job.logprint('images_prepped\n')
                this_job.logprint(''.join(["Event= ", str(new_event.event_id),"\n",detname,"\n","images_prepped\n"]))
                time.sleep(1)
            '''
            try:
                detname = this_event.options['detname']
            except:
                detname = this_target.name
                my_job.logprint(''.join(["FAILED TEST event detname is ", str(detname)]))
            new_event = this_job.child_event('images_prepped', tag=detname, options={'target_id': tid,'detname': detname,'submission_type': 'scheduler'})
            this_job.logprint('about to fire')
            this_job.logprint(''.join(["event detname is ", str(detname)]))
            new_event.fire()
            this_job.logprint('fired')
            this_job.logprint('images_prepped\n')
            this_job.logprint(''.join(["Event= ", str(new_event.event_id),"\n",detname,"\n","images_prepped\n"]))

