import abc
import threading

class BaseScheduler(abc.ABC):
    """
    Inherit this abstract class for a non-blocking timer.  Call reset() to
    reset the timer.

    Override the abstract method _execute for the timer to call.
    """

    def __init__(self):
        threading.Timer(2, self._middleman).start()
        self.reset = False

    def _middleman(self):
        if (self.reset == True):
            self.reset = False
            threading.Timer(2, self._middleman).start()
        else:
            self._execute()

    @abc.abstractmethod
    def _execute(self):
        pass

    def reset(self):
        self.reset = True