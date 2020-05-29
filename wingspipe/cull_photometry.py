#! /usr/bin/env python
import wpipe as wp


def register(task):
    _temp = task.mask(source='*', name='start', value='*')
    _temp = task.mask(source='test_wpipe.py', name='test', value='*')


def create_target(a=''):
    pass


def parse_all():
    parser = wp.PARSER
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_all()
    config = pipeline
    targs, data, config = discover_targets()
    count = 0
    for targ in targs:
        pass
        # create_target(targ,data[count,:],config)
        # count++
