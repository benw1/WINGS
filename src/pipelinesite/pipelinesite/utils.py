from enum import Enum, auto


class DataProductGroups(Enum):
    RAW = "raw"
    PROCESS = "proc"
    LOG = "log"
    CONFIGURATION = "conf"

class JobStates(Enum):
    SUBMITTED = ("Submitted", 2)
    INITIALIZED = ("Initialized", 3)
    COMPLETED = ("Completed", 4)
    KEYBOARD_INTERRUPT = ("KeyboardInterrupt()", 1)
    ERROR = ("Error", 0)  # This is a catch all for anything unkown, which is likely an error

    @classmethod
    def fromValue(cls, value):
        print("VALUE:", value)
        if value == JobStates.SUBMITTED.value:
            return JobStates.SUBMITTED.value[1]
        elif value == JobStates.INITIALIZED.value:
            return JobStates.INITIALIZED.value[1]
        elif value == JobStates.COMPLETED.value:
            return JobStates.COMPLETED.value[1]
        elif value == JobStates.KEYBOARD_INTERRUPT.value:
            return JobStates.KEYBOARD_INTERRUPT.value[1]
        else:
            return JobStates.ERROR.value[1]
