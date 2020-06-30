#! /usr/bin/env python
import os
#import subprocess
from stips.observation_module import ObservationModule
import numpy as np
import wpipe as wp

on_hyak = False
on_pbs = False
filtdict = {'R': 'F062',
            'Z': 'F087',
            'Y': 'F106',
            'J': 'F129',
            'H': 'F158',
            'F': 'F184'}


def register(task):
    _temp = task.mask(source='*', name='start', value=task.name)
    _temp = task.mask(source='*', name='new_stips_catalog', value='*')


def hyak_stips(event_id, dp_id, stips_script):
    my_event = wp.Event(event_id)
    catalog_id = my_event.options['dp_id']
    catalog_dp = wp.DataProduct(catalog_id)
    my_config = catalog_dp.config
    slurmfile = stips_script + '.slurm'
    with open(slurmfile, 'w') as f:
        f.write('#!/bin/bash' + '\n' +
                '## Job Name' + '\n' +
                '#SBATCH --job-name=stips' + str(dp_id) + '\n' +
                '## Allocation Definition ' + '\n' +
                '#SBATCH --account=astro' + '\n' +
                '#SBATCH --partition=astro' + '\n' +
                '## Resources' + '\n' +
                '## Nodes' + '\n' +
                '#SBATCH --ntasks=1' + '\n' +
                '## Walltime (3 hours)' + '\n' +
                '#SBATCH --time=10:00:00' + '\n' +
                '## Memory per node' + '\n' +
                '#SBATCH --mem=10G' + '\n' +
                '## Specify the working directory for this job' + '\n' +
                '#SBATCH --workdir=' + my_config.procpath + '\n' +
                '##turn on e-mail notification' + '\n' +
                'source activate forSTIPS3' + '\n' +
                'python ' + stips_script)
    subprocess.run(['sbatch', slurmfile], cwd=my_config.procpath)


def pbs_stips(event_id, dp_id, stips_script):
    my_event = wp.Event(event_id)
    catalog_id = my_event.options['dp_id']
    catalog_dp = wp.DataProduct(catalog_id)
    my_config = catalog_dp.config
    filename = str(catalog_dp.filename)  # for example, Mixed_h15_shell_3Mpc_Z.tbl
    filebase = filename.split('.')[0]
    # pbsfile = stips_script+'.pbs'
    pbsfile = '/home1/bwilli24/Wpipelines/run_stips_jobs'
    # with open(pbsfile, 'w') as f:
    with open(pbsfile, 'a') as f:
        f.write('#PBS -S /bin/bash' + '\n' +
                '#PBS -j oe' + '\n' +
                '#PBS -l select=1:ncpus=4:model=san' + '\n' +
                '#PBS -W group_list=s1692' + '\n' +
                '#PBS -l walltime=10:00:00' + '\n' +
                'source ~/.bashrc' + '\n' +
                'cd ' + my_config.procpath + '\n' +

                'source /nobackupp11/bwilli24/miniconda3/bin/activate STIPS' + '\n' +
                'source /nobackupp11/bwilli24/miniconda3/bin/activate STIPS && ' + 'cd /tmp' +
                ' && python ' + stips_script +
                ' && mv /tmp/sim*' + str(dp_id) + '*fits ' + my_config.procpath +
                ' && mv /tmp/' + filebase + '* ' + my_config.procpath + '\n')
        # 'source /nobackupp11/bwilli24/miniconda3/bin/activate STIPS && ' + 'cd ' + my_config.procpath +
        # ' && python ' + stips_script + '\n')
        # 'python '+stips_script)
    return
    # subprocess.run(['qsub',pbsfile],cwd=my_config.procpath)

'''
def run_stips(event_id, dp_id, ra_dith, dec_dith, run_id):
    catalog_dp = wp.DataProduct(dp_id)
    my_config = catalog_dp.config
    my_params = my_config.parameters
    racent = float(my_params['racent']) + (float(ra_dith) / 3600.0)
    deccent = float(my_params['deccent']) + (float(dec_dith) / 3600.0)
    try:
        pa = my_params['pa']
    except KeyError:
        pa = 0.0
    fileroot = str(catalog_dp.relativepath)
    filename = str(catalog_dp.filename)  # for example, Mixed_h15_shell_3Mpc_Z.tbl
    filtroot = filename.split('_')[-1].split('.')[0]
    filtername = filtdict[filtroot]
    stips_script = my_config.confpath + '/run_stips_' + str(dp_id) + '.py'
    with open(stips_script, 'w') as f:
        f.write('from stips.observation_module import ObservationModule' + '\n' +
                'import numpy as np\n' +
                'import os\n' +
                'os.chdir(\'' + my_config.procpath + '\')\n' +
                'filename = \'' + fileroot + '/' + filename + '\'\n' +
                'seed = np.random.randint(9999)+1000' + '\n' +
                'with open(filename) as myfile:' + '\n' +
                '   head = [next(myfile) for x in range(3)]' + '\n' +
                'pos = head[2].split(\' \')' + '\n' +
                'crud,ra = pos[2].split(\'(\')' + '\n' +
                'dec,crud =  pos[4].split(\')\')' + '\n' +
                'print(\"Running \",filename,ra,dec)' + '\n' +
                'print(\"SEED \",seed)' + '\n' +
                'scene_general = ' +
                '{\'ra\': ' + str(racent) + ', \'dec\': ' + str(deccent) + ',' +
                ' \'pa\': ' + str(pa) + ', \'seed\': seed}' + '\n' +
                'obs = ' +
                '{\'instrument\': \'WFI\', \'filters\': [\'' + filtername + '\'], \'detectors\': 1,' +
                ' \'distortion\': False, \'oversample\': ' + str(my_params['oversample']) + ', \'pupil_mask\': \'\',' +
                ' \'background\': \'avg\', \'observations_id\': ' + str(dp_id) + ',' +
                ' \'exptime\': ' + str(my_params['exptime']) + ',' +
                ' \'offsets\': [' +
                '{\'offset_id\': ' + str(run_id) + ', \'offset_centre\': False,' +
                ' \'offset_ra\': 0.0, \'offset_dec\': 0.0, \'offset_pa\': 0.0}]}' + '\n' +
                'obm = ObservationModule(obs, scene_general=scene_general)' + '\n' +
                'obm.nextObservation()' + '\n' + 'source_count_catalogues = obm.addCatalogue(str(filename))' + '\n' +
                'psf_file = obm.addError()' + '\n' +
                'fits_file, mosaic_file, params = obm.finalize(mosaic=False)' + '\n')
    if on_hyak:
        hyak_stips(event_id, dp_id, stips_script)
    elif on_pbs:
        pbs_stips(event_id, dp_id, stips_script)
    else:
        os.system("python " + stips_script)
    _dp = my_config.dataproduct(filename='sim_' + str(dp_id) + '_0.fits', relativepath=fileroot,
                                group='proc', subtype='stips_image',
                                filtername=filtername, ra=my_params['racent'], dec=my_params['deccent'])

'''

def run_stips(event_id, dp_id, ra_dith, dec_dith):
    catalog_dp = wp.DataProduct(dp_id)
    my_config = catalog_dp.config
    my_params = my_config.parameters
    racent = float(my_params['racent']) + (float(ra_dith) / 3600.0)
    deccent = float(my_params['deccent']) + (float(dec_dith) / 3600.0)
    try:
        pa = my_params['pa']
    except KeyError:
        pa = 0.0
    fileroot = str(catalog_dp.relativepath)
    filename = str(catalog_dp.filename)  # for example, Mixed_h15_shell_3Mpc_Z.tbl
    filtroot = filename.split('_')[-1].split('.')[0]
    filtername = filtroot
    os.chdir(my_config.procpath)
    filename = fileroot + '/' + filename 
    seed = np.random.randint(9999)+1000
    with open(filename) as myfile:
       head = [next(myfile) for x in range(3)]
    pos = head[2].split(' ')
    crud,ra = pos[2].split('(')
    dec,crud =  pos[4].split(')')
    print("Running ",filename,ra,dec)
    print("SEED ",seed)
    scene_general = {'ra': racent, 'dec': deccent, 'pa': pa, 'seed': seed}
    obs = {'instrument': 'WFI', 'filters': [filtername], 'detectors': my_params['detectors'], 'distortion': False, 'oversample': my_params['oversample'], 'pupil_mask': '', 'background': 'avg', 'observations_id': dp_id, 'exptime': my_params['exptime'], 'offsets': [{'offset_id': event_id, 'offset_centre': False, 'offset_ra': 0.0, 'offset_dec': 0.0, 'offset_pa': 0.0}]}
    obm = ObservationModule(obs, scene_general=scene_general)
    obm.nextObservation()
    source_count_catalogues = obm.addCatalogue(str(filename))
    psf_file = obm.addError()
    fits_file, mosaic_file, params = obm.finalize(mosaic=False)
    _dp = my_config.dataproduct(filename='sim_' + str(dp_id) + '_0.fits', relativepath=fileroot,
                                group='proc', subtype='stips_image',
                                filtername=filtername, ra=my_params['racent'], dec=my_params['deccent'])




def parse_all():
    parser = wp.PARSER
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_all()
    this_job_id = args.job_id
    this_job = wp.Job(this_job_id)
    this_event = this_job.firing_event
    this_event_id = this_event.event_id
    this_dp_id = this_event.options['dp_id']
    parent_job_id = this_event.parent_job_id
    parent_job = this_event.parent_job
    compname = this_event.options['name']
    ra_dither = this_event.options['ra_dither']
    dec_dither = this_event.options['dec_dither']
    run_stips(this_event_id, this_dp_id, float(ra_dither), float(dec_dither))
    update_option = parent_job.options[compname]
    update_option = update_option + 1
    parent_job.options[compname] = update_option
    to_run = this_event.options['to_run']
    completed = update_option
    catalogID = this_event.options['dp_id']
    catalogDP = wp.DataProduct(catalogID)
    this_conf = catalogDP.config
    this_target = this_conf.target
    print(''.join(["Completed ", str(completed), " of ", str(to_run)]))
    this_job.logprint(''.join(["Completed ", str(completed), " of ", str(to_run), "\n"]))
    if completed >= to_run:
        this_job.logprint(''.join(["Completed ", str(completed), " and to run is ", str(to_run), " firing event\n"]))
        DP = wp.DataProduct(this_dp_id)
        tid = DP.target_id
        path = this_conf.procpath
        image_dps = wp.DataProduct.select(config_id=this_conf.config_id, subtype="stips_image")
        comp_name = 'completed' + this_target.name
        options = {comp_name: 0}
        this_job.options = options
        total = len(image_dps)
        # print(image_dps(0))
        for dps in image_dps:
            print(dps)
            dpid = dps.dp_id
            new_event = this_job.child_event('stips_done', tag=dps.filtername,
                                             options={'target_id': tid, 'dp_id': dpid,
                                                      'name': comp_name, 'to_run': total})
            new_event.fire()
            # this_job.logprint('stips_done but not firing any events for now\n')
            this_job.logprint(''.join(["Event= ", str(this_event.event_id)]))
