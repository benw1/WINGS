import abc
import threading

class BaseScheduler(abc.ABC):
    """
    Inherit this abstract class for a non-blocking timer.  Call reset() to
    reset the timer.

    Override the abstract method _execute for the timer to call.
    """

    def __init__(self, timer=20):
        self.timer = timer
        self.reset = False

        threading.Timer(self.timer, self._middleman).start()

    def _middleman(self):
        if self.reset:
            self.reset = False
            threading.Timer(self.timer, self._middleman).start()
        else:
            self._execute()

    @abc.abstractmethod
    def _execute(self):
        pass

    def reset(self):
        self.reset = True
