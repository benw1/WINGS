from typing import Union

from django import template

from pipelinesite.models import Jobs, Events

register = template.Library()

# @register.simple_tag
# def parent_event(firing_event) -> str:
#     print(firing_event.parent_job)
#     print('hello world')
#     print(firing_event.parent_job is None)
#     print(firing_event.parent_job.fFiring_event is None)
#     if firing_event.parent_job is None and firing_event.parent_job.firing_event is None:
#         return "None"
#     return firing_event.parent_job.firing_event.name

@register.simple_tag
def create_href_from_model(model: Union[Jobs, Events]) -> str:
    if isinstance(model, Jobs):
        pass
    if isinstance(model, Events):
        return f"url 'manager:event_detail_view' {model.pk}"
