#! /usr/bin/env python
import wpipe as wp


def register(task):
    _temp = task.mask(source='*', name='start', value=task.name)


def parse_all():
    parser = wp.PARSER
    parser.add_argument('--config', '-c', type=str, dest='config_id',
                        help='Configuration ID to be copied')
    parser.add_argument('--job', '-j', type=int, dest='job_id',
                        help='Job ID')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_all()
    if args.config_id is None:
        exit("Need to define a configuration ID")
    config_id = args.config_id

    # Fetch old config and the target
    oldConfig = wp.SQLConfiguration(config_id)
    target = oldConfig.target

    # Fetch, update params
    params = oldConfig.parameters
    params['oversample'] = 4

    # Create new config object in memory
    newConfig = target.configuration('os4', parameters=params)

    print("OLD: ", oldConfig, "\n", "New: ", newConfig)
    # placeholder for additional steps
    print('done')
