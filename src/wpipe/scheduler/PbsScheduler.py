from .BaseScheduler import BaseScheduler

class PbsScheduler(BaseScheduler):

    # Keep track of all the instances that might be spawned
    schedulers = list()

    def __init__(self, key):
        super().__init__()
        print("Create a new scheduler")
        self.key = key
        PbsScheduler.schedulers.append(self) # add this new scheduler to the list

        # run the submit now that the object is created
        self._submit()

    #######################
    ## Internal Use Only ##
    #######################

    def _submit(self):
        # TODO: Probably need a pass in variable
        print("do a reset")
        super().reset()


    def _execute(self):
        print("We do the scheduling now from: " + self.key)
        # remove scheduler from list
        PbsScheduler.schedulers.remove(self)


    @staticmethod
    def _checkForScheduler(key):
        for scheduler in PbsScheduler.schedulers:
            if (scheduler.key == key):
                return (True, scheduler)
        return (False, None)

    ######################
    ### Usable Methods ###
    ######################

    @staticmethod
    def submit(event, job):
        # Check if any scheduler exists.
        print(event)
        print(job)
        if (len(PbsScheduler.schedulers) == 0):  # !self._checkScheduler(stuff)):
            PbsScheduler(event)
            return

        (hasScheduler, scheduler) = PbsScheduler._checkForScheduler(key)
        if (hasScheduler): # check schedulers and submit to one
            print("A scheduler with those attributes exists")
            scheduler._submit()
        else: # No scheduler was found but we need to do the scheduling
            PbsScheduler(event)
