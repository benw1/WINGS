import django_filters

from pipelinesite.models import Events

class EventFilter(django_filters.FilterSet):

    class Meta:
        model = Events

        fields = {
            'name': ['icontains'],
        }
