
def get_parent_jobs(base_job: 'wpipe.Job', parent_count: int):
    current_job = base_job
    for i in parent_count:
        if current_job == None:
            return

        # Get the parent job


        # Set the current_job to the parent_job