import django_filters
from django_filters.widgets import RangeWidget

from pipelinesite.models import Jobs

class JobFilter(django_filters.FilterSet):

    start_time = django_filters.DateFromToRangeFilter(field_name='starttime',
                                                      label='Start time',
                                                      widget=RangeWidget(attrs={'type': 'date'}))


    class Meta:
        model = Jobs

        fields = {
            'state': ['icontains'],
            'node__name': ['icontains'],
            'task__name': ['icontains'],
            'firing_event__name': ['icontains'],
        }

