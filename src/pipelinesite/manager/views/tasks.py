from django.core.paginator import Paginator
from django.shortcuts import render

from manager.filters.task_filter import TaskFilter
from pipelinesite.models import Tasks


def task_list(request):

    all_tasks = Tasks.objects.all()

    task_filter = TaskFilter(request.GET, queryset=all_tasks)

    paginator = Paginator(task_filter.qs, 20)

    page_number = request.GET.get('page')
    if not page_number:
        page_number = 1

    page_obj = paginator.get_page(page_number)

    end_range = int(page_number) + 4

    if end_range > page_obj.end_index():
        end_range = page_obj.end_index()
    pagination_range = range(int(page_number), end_range)

    context = {'task_page': page_obj,
               'pagination_range': pagination_range,
               'total_pages': paginator.num_pages,
               'task_filter': task_filter,
               }
    return render(request, 'task/task_list_view.html', context)


def task_detail_view(request, pk):
    task = Tasks.objects.get(pk=pk)

    context = {'task': task}
    return render(request, 'task/task_detail_view.html', context)
