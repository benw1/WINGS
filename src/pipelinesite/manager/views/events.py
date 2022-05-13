from django.core.paginator import Paginator
from django.shortcuts import render

from manager.filters.event_filter import EventFilter
from pipelinesite.models import Events


def event_list(request):

    all_events = Events.objects.all()

    event_filter = EventFilter(request.GET, queryset=all_events)

    paginator = Paginator(event_filter.qs, 20)

    page_number = request.GET.get('page')
    if not page_number:
        page_number = 1

    page_obj = paginator.get_page(page_number)

    end_range = int(page_number) + 4

    if end_range > page_obj.end_index():
        end_range = page_obj.end_index()
    pagination_range = range(int(page_number), end_range)

    context = {'event_page': page_obj,
               'pagination_range': pagination_range,
               'total_pages': paginator.num_pages,
               'event_filter': event_filter,
               }
    return render(request, 'event/event_list_view.html', context)


def event_detail_view(request, pk):
    event = Events.objects.get(pk=pk)

    context = {'event': event}
    return render(request, 'event/event_detail_view.html', context)
