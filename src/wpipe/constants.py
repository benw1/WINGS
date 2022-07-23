import os

WPIPE_NO_SCHEDULER = os.getenv('WPIPE_NO_SCHEDULER') is not None
LOGPRINT_TIMESTAMP = os.getenv('WPIPE_LOGPRINT_TIMESTAMP') is not None
