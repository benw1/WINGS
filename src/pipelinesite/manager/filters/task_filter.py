import django_filters
from django_filters.widgets import RangeWidget

from pipelinesite.models import Tasks

class TaskFilter(django_filters.FilterSet):
    time_stamp = django_filters.DateFromToRangeFilter(field_name='timestamp',
                                                      label='Timestamp',
                                                      widget=RangeWidget(attrs={'type': 'date'}))
    run_time = django_filters.NumericRangeFilter(field_name='run_time',
                                                 label='Run Time')
    class Meta:
        model = Tasks

        fields = {
            'name': ['icontains'],
            # 'run_time': ['exact'],
            'pipeline__name': ['icontains'],
        }
