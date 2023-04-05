#!/usr/bin/env python
"""
Contains the SlurmConsumer utilities including the scheduler.checkSlurmConnection
and scheduler.sendJobToSlurm function definitions

Please note that this module is private. These functions are available in the
main ``wpipe.scheduler`` namespace - use that instead.
"""
from .BaseConsumer import checkConsumerConnection, sendJobToConsumer, consumer_main

__all__ = ['checkSlurmConnection', 'sendJobToSlurm']


checkSlurmConnection = checkConsumerConnection


sendJobToSlurm = sendJobToConsumer


if __name__ == "__main__":
    consumer_main(kind='Slurm')
