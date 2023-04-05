#!/usr/bin/env python
"""
Contains the PbsConsumer utilities including the scheduler.checkPbsConnection
and scheduler.sendJobToPbs function definitions

Please note that this module is private. These functions are available in the
main ``wpipe.scheduler`` namespace - use that instead.
"""
from .BaseConsumer import checkConsumerConnection, sendJobToConsumer, consumer_main

__all__ = ['checkPbsConnection', 'sendJobToPbs']


checkPbsConnection = checkConsumerConnection


sendJobToPbs = sendJobToConsumer


if __name__ == "__main__":
    consumer_main(kind='Pbs')
