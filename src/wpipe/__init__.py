#!/usr/bin/env python
"""

Description
-----------

Provides a suite of classes to deploy the WINGS pipeline functionalities.

How to use
----------

Wpipe handles the building and running of a WINGS pipeline and track its
jobbing through a shared SQL database. It does so via a suite of classes that
represent each of the SQL database tables and the necessary entities of a
WINGS pipeline. These classes represent namely:
- the user that owns the pipeline (User),
- the pipeline itself (Pipeline),
- the tasks that compose the pipeline software (Task),
- the masks that are associated to a task (Mask)
- the input set of data given to the pipeline (Input),
- the targets that an input contains (Target),
- the configuration for such targets (Configuration),
- the parameters that constitute a configuration (Parameter),
- the dataproducts that constitute an input or a configuration (DataProduct),
- the jobs that are submitted by the pipeline from specific events (Job),
- the events that are fired by the pipeline jobbing (Event),
- the options that may be given to any target, job, event or dataproduct
(Option).

The core running of the pipeline is handled by its jobbing. This goes through
the firing of events that search for tasks with matching masks. These events
subsequently submit those tasks as new jobs that themselves fire new events to
submit other jobs. This chaining between events submitting jobs and jobs
firing events constitutes the pipeline running. When a job is submitted, the
script of the corresponding task is executed through the system or submitted
to the scheduler with command-line arguments recognized by Wpipe PARSER. For
this reason, the script must be beforehand coded so that it imports the Wpipe
module and use that PARSER.

Among these command-line arguments, the key id in the SQL database of the
corresponding submitted job is passed through and shall be used to construct
the corresponding Job object in that new python instance to fire new events.
The shared SQL database also help to further communicate between python
instances notably via the options that can be assigned to the jobs and events.
Each of the Wpipe classes have written-in documentations for further
instructions for how to use them.

+ wingspipe

To exploit Wpipe functionalities, it is recommended to use the command-line
executable wingspipe, installed with Wpipe. 2 things need to be prepared
before calling the command: the set of tasks that will build the pipeline
software and the set of data input that the software will run with.

For the former set of tasks, these shall be python scripts with read and
execution permissions, and with the only requirement in the format that it
must implement a single-argument function register. This function gives the
possibility to set Mask objects to the corresponding Task, treating the latter
as the argument of the function. All these tasks must be placed in a same
directory, which path must be given to the wingspipe command-line argument
--tasks_path or -w.

In the case of the data input, if a single data file is needed, then the path
to this file must be given to the wingspipe command-line argument --inputs or
-i. If more files are needed, it is also possible to put all of them with
characterizing names in a directory and enter its path in the command-line
argument of wingspipe. This directory can also contain any configuration file
with extension '.conf', otherwise configuration files can also be added to the
--config or -c command-line argument.

The wingspipe command may be used as many times as necessary to add more tasks
or inputs to the pipeline. Once the pipeline is completely built, it can be
started by adding the wingspipe command-line flag --run or -r. This will call
the Pipeline object method run_pipeline which submits the pipeline dummy_job,
firing an event with name '__init__'. Accordingly with the Wpipe jobbing, when
fired, this event searches for the task with mask of name '__init__', meaning
that the script corresponding to the first task to be submitted in the
pipeline shall be written to register this mask.

+ sqlintf

Of the Wpipe classes, each constructed object ultimately represents a row of
the corresponding SQL table after construction. The constructor notably
queries the database for rows with key column entries that correspond to those
given in its call signature, or create a new row if that query doesn't return.
In this way, each object made out of that suite of classes uniquely represents
a row of the shared SQL database in a single python instance, and conversely.

The entire connection with the SQL database is powered by the third-party
module SQLAlchemy and is implemented via the subpackage sqlintf. In practice,
no one should ever need to use this subpackage, it contains the tools to
initialize the database connection and query it, as well as duplicates of each
of the Wpipe classes. These duplicates form the SQL interface for the Wpipe
classes, as in constructing an object of these duplicates is equivalent to
adding a new row to the database table. These duplicates also are the classes
of the returned object when querying the database for existing rows:
accordingly, the Wpipe classes have been coded to query for existing rows, or
to create new rows otherwise.

Available subpackages
---------------------
sqlintf
    Suite of classes connected to the database tables
    - powered by the module `SQLAlchemy`

Utilities
---------
PARSER
    pre-instantiated parser powered by the module `argparse`

DefaultUser
    User object constructed at wpipe importation (see User doc Notes)

DefaultNode
    Node object constructed at wpipe importation (see Node doc Notes)

wingspipe
    Function that runs the wingspipe executable functionalities

__version__
    Wpipe version string
"""
import pathlib
import sys

from .__metadata__ import *
from .constants import WPIPE_NO_SCHEDULER
from .core import *
from .User import User
from .Node import Node
from .Pipeline import Pipeline
from .Input import Input
from .Option import Option
from .Target import Target
from .Configuration import Configuration
from .Parameter import Parameter
from .DataProduct import DataProduct
from .Task import Task
from .Mask import Mask
from .Job import Job
from .Event import Event
from .scheduler import PbsConsumer
from .scheduler import SlurmConsumer
from .scheduler.ConsumerFactory import get_consumer_factory

__all__ = ['__version__', 'PARSER', 'User', 'Node', 'Pipeline', 'Input',
           'Option', 'Target', 'Configuration', 'Parameter', 'DataProduct',
           'Task', 'Mask', 'Job', 'Event',
           'DefaultUser', 'DefaultNode', 'wingspipe']


warnings.filterwarnings("ignore", message=".*Cannot correctly sort tables;.*")

DefaultUser = User()
"""
User object: User object constructed at wpipe importation (see User doc Notes)
"""

PbsConsumer.DEFAULT_PORT = PbsConsumer.BASE_PORT + DefaultUser.user_id
SlurmConsumer.DEFAULT_PORT = SlurmConsumer.BASE_PORT + DefaultUser.user_id

DefaultNode = Node()
"""
Node object: Node object constructed at wpipe importation (see Node doc Notes)
"""

if pathlib.Path(sys.argv[0]).resolve().name != 'wingspipe':
    if PARSER.parse_known_args()[0].event_id is not None or PARSER.parse_known_args()[0].job_id is not None:
        if PARSER.parse_known_args()[0].event_id is not None:
            ThisEvent = Event()
            if ThisEvent.fired_jobs[-1].is_active if len(ThisEvent.fired_jobs) else False:
                print("Event with id %d has a job attempt that is currently running - exiting" % ThisEvent.event_id)
                sys.exit()
            else:
                ThisJob = ThisEvent._generate_new_job(Task(ThisEvent.pipeline, os.path.basename(sys.argv[0])))
                sys.argv += ['-j', str(ThisJob.job_id)]  # MEH
        elif PARSER.parse_known_args()[0].job_id is not None:
            ThisJob = Job()
            ThisEvent = ThisJob.firing_event
            if ThisJob.is_active:
                print("Job with id %d is currently running - exiting" % ThisJob.job_id)
                sys.exit()
            elif not ThisJob.not_submitted:
                ThisJob.reset()
        ThisJob._starting_todo()
        atexit.register(ThisJob._ending_todo)


# TODO: Delete?
# def sql_hyak(task, job_id, event_id):
#     my_job = Job(job_id)
#     my_pipe = my_job.pipeline
#     swroot = my_pipe.software_root
#     executable = swroot + '/' + task.name
#     catalog_id = Event(event_id).options['dp_id']
#     catalog_dp = DataProduct(catalog_id)
#     my_config = catalog_dp.config
#     slurmfile = my_config.confpath + '/' + task.name + '_' + str(job_id) + '.slurm'
#     # print(event_id,job_id,executable,type(executable))
#     eidstr = str(event_id)
#     jidstr = str(job_id)
#     print("Submitting ", slurmfile)
#     with open(slurmfile, 'w') as f:
#         f.write('#!/bin/bash' + '\n' +
#                 '## Job Name' + '\n' +
#                 '#SBATCH --job-name=' + jidstr + '\n' +
#                 '## Allocation Definition ' + '\n' +
#                 '#SBATCH --account=astro' + '\n' +
#                 '#SBATCH --partition=astro' + '\n' +
#                 '## Resources' + '\n' +
#                 '## Nodes' + '\n' +
#                 '#SBATCH --ntasks=1' + '\n' +
#                 '## Walltime (10 hours)' + '\n' +
#                 '#SBATCH --time=10:00:00' + '\n' +
#                 '## Memory per node' + '\n' +
#                 '#SBATCH --mem=10G' + '\n' +
#                 '## Specify the working directory for this job' + '\n' +
#                 '#SBATCH --workdir=' + my_config.procpath + '\n' +
#                 'source activate forSTIPS3' + '\n' +
#                 executable + ' -e ' + eidstr + ' -j ' + jidstr)
#     subprocess.run(['sbatch', slurmfile], cwd=my_config.confpath)
#
#
# def sql_pbs(task, job_id, event_id):
#     my_job = Job(job_id)
#     my_pipe = my_job.pipeline
#     swroot = my_pipe.software_root
#     executable = swroot + '/' + task.name
#     catalog_id = Event(event_id).options['dp_id']
#     catalog_dp = DataProduct(catalog_id)
#     my_config = catalog_dp.config
#     # pbsfile = my_config.confpath + '/' + task.name + '_' + str(job_id) + '.pbs'
#     pbsfile = '/home1/bwilli24/Wpipelines/' + task.name + '_jobs'
#     # print(event_id,job_id,executable,type(executable))
#     eidstr = str(event_id)
#     jidstr = str(job_id)
#     print("Submitting ", pbsfile)
#     # with open(pbsfile, 'w') as f:
#     with open(pbsfile, 'a') as f:
#         f.write(  # '#PBS -S /bin/csh' + '\n'+
#             # '#PBS -j oe' + '\n'+
#             # '#PBS -l select=1:ncpus=4:model=san' + '\n'+
#             # '#PBS -W group_list=s1692' + '\n'+
#             # '#PBS -l walltime=10:00:00' + '\n'+
#
#             # 'cd ' + myConfig.procpath  + '\n'+
#
#             # 'source activate STIPS'+'\n'+
#
#             # executable+' -e '+eidstr+' -j '+jidstr)
#             'source /nobackupp11/bwilli24/miniconda3/bin/activate STIPS && ' +
#             executable + ' -e ' + eidstr + ' -j ' + jidstr + '\n')
#     subprocess.run(['qsub', pbsfile], cwd=my_config.confpath)


def wingspipe(args=None):
    """
    Function that runs the wingspipe executable functionalities.

    Parameters
    ----------
    args : list
        List of command-line arguments the executable would use - defaults
        to sys.argv[1:].

    Notes
    -----
    This function is called in the executable wingspipe installed with Wpipe.
    By default, using it in a python environment reads in the command-line
    arguments given to the script that calls it. This can be changed by giving
    the function parameter args the list of arguments the executable would use
    as a list pre-split string as in this example:

    >>> wingspipe(['-w', './tasks',
    >>>            '-d', 'A new pipeline',
    >>>            '-i', './inputs',
    >>>            '-c', './default.conf',
    >>>            '-r'])
    """
    if args is not None:
        sys.argv += args  # MEH
    # _temp = PbsConsumer.DEFAULT_PORT
    importlib.reload(sys.modules[__name__])
    # PbsConsumer.DEFAULT_PORT = _temp
    parent_parser = si.argparse.ArgumentParser(parents=[PARSER], add_help=False)
    parser = si.argparse.ArgumentParser(prog='wingspipe', parents=[si.PARSER], add_help=False)
    subparsers = parser.add_subparsers()
    parser_init = subparsers.add_parser('init', parents=[parent_parser], add_help=False)
    parser_init.set_defaults(which='init')
    parser_init.add_argument('--tasks_path', '-w', dest='tasks_path', default=None,
                             help='Path to pipeline tasks to be registered')
    parser_init.add_argument('--description', '-d', dest='description', default='',
                             help='Optional description of this pipeline')
    parser_init.add_argument('--inputs', '-i', type=str, dest='inputs_path',
                             help='Path to directory with input lists')
    parser_init.add_argument('--config', '-c', type=str, dest='config_file',
                             help='Configuration File Path')
    parser_init.add_argument('--run', '-r', dest='run', action='store_true',
                             help='Run the pipeline')
    parser_run = subparsers.add_parser('run', parents=[parent_parser], add_help=False)
    parser_run.set_defaults(which='run')
    parser_diagnose = subparsers.add_parser('diagnose', parents=[parent_parser], add_help=False)
    parser_diagnose.set_defaults(which='diagnose')
    parser_expire = subparsers.add_parser('expire', parents=[parent_parser], add_help=False)
    parser_expire.set_defaults(which='expire')
    parent_parser_with_yes_flag = si.argparse.ArgumentParser(parents=[parent_parser], add_help=False)
    parent_parser_with_yes_flag.add_argument('--yes', '-y', dest='yes', action='store_true',
                                             help="Don't ask for confirmation")
    parser_reset = subparsers.add_parser('reset', parents=[parent_parser_with_yes_flag], add_help=False)
    parser_reset.set_defaults(which='reset')
    parser_clean = subparsers.add_parser('clean', parents=[parent_parser_with_yes_flag], add_help=False)
    parser_clean.set_defaults(which='clean')
    parser_delete = subparsers.add_parser('delete', parents=[parent_parser_with_yes_flag], add_help=False)
    parser_delete.add_argument('--force', '-f', dest='force', action='store_true',  # TODO
                               help="Force deletion of every files")
    parser_delete.set_defaults(which='delete')
    args = parser.parse_args()
    if hasattr(args, 'which'):
        if args.which == 'expire':
            Job(args.job_id).expire()
        else:
            my_pipe = Pipeline()
            command = parser.prog + " " + args.which
            if args.which == 'init':
                my_pipe.description = args.description
                my_pipe.attach_tasks(args.tasks_path)
                my_pipe.attach_inputs(args.inputs_path, args.config_file)
            elif args.which == 'run':
                if not WPIPE_NO_SCHEDULER:
                    consumer = get_consumer_factory()
                    consumer('start')
                # TODO if args.event_id or args.job_id
                my_pipe.run()

            elif args.which == 'diagnose':
                my_pipe.diagnose()
            elif args.which == 'reset':
                if True if args.yes \
                        else input(command + ': confirm reset of pipeline at ' +
                                   my_pipe.pipe_root + '? [y/yes] ') in ['y', 'yes']:
                    my_pipe.reset()
            elif args.which == 'clean':
                if True if args.yes \
                        else input(command + ': confirm clean-up of pipeline at ' +
                                   my_pipe.pipe_root + '? [y/yes] ') in ['y', 'yes']:
                    my_pipe.clean()
            elif args.which == 'delete':
                if True if args.yes \
                        else input(command + ': confirm deletion of pipeline at ' +
                                   my_pipe.pipe_root + '? [y/yes] ') in ['y', 'yes']:
                    my_pipe.delete()  # TODO: my_pipe.delete(force = args.force)
    else:
        parser.print_help()
