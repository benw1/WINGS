from django.db.models import F, Value, IntegerField, Case, When
from django.shortcuts import render
from django.core.paginator import Paginator

from pipelinesite.models import Jobs
from pipelinesite.utils import JobStates

def jobs(request):

    # TODO: Add child task.  The current task in job is the parent task.  Code for trying to get the child task
    # -> my_job.parent_job.task
    # -> my_event.parent_job.task

    all_jobs = Jobs.objects.\
        annotate(state_integer=Case(
            When(state=JobStates.SUBMITTED.value[0], then=Value(JobStates.SUBMITTED.value[1])),
            When(state=JobStates.INITIALIZED.value[0], then=Value(JobStates.INITIALIZED.value[1])),
            When(state=JobStates.COMPLETED.value[0], then=Value(JobStates.COMPLETED.value[1])),
            When(state=JobStates.KEYBOARD_INTERRUPT.value[0], then=Value(JobStates.KEYBOARD_INTERRUPT.value[1])),
            default=Value(JobStates.ERROR.value[1])
    )).order_by('state_integer', 'starttime')

    paginator = Paginator(all_jobs, 20)

    page_number = request.GET.get('page')
    if not page_number:
        page_number = 1

    page_obj = paginator.get_page(page_number)

    end_range = int(page_number) + 4

    if end_range > page_obj.end_index():
        end_range = page_obj.end_index()
    pagination_range = range(int(page_number), end_range)



    context = {'jobs_page': page_obj, 'pagination_range': pagination_range, 'total_pages': paginator.num_pages}
    return render(request, 'jobs.html', context)

def job_single_view(request, pk):
    job = Jobs.objects.get(pk=pk)

    context = {'job': job}
    return render(request, 'job_single_view.html', context)
