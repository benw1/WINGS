from typing import Union

from django import template
# from wpipe.sqlintf import Job, Event, Node, Configuration

register = template.Library()


@register.simple_tag(takes_context=True)
def param_replace(context, **kwargs):
    """
    Return encoded URL parameters that are the same as the current
    request's parameters, only with the specified GET parameters added or changed.

    It also removes any empty parameters to keep things neat,
    so you can remove a parm by setting it to ``""``.

    For example, if you're on the page ``/things/?with_frosting=true&page=5``,
    then

    <a href="/things/?{% param_replace page=3 %}">Page 3</a>

    would expand to

    <a href="/things/?with_frosting=true&page=3">Page 3</a>

    Based on
    https://stackoverflow.com/questions/22734695/next-and-before-links-for-a-django-paginated-query/22735278#22735278
    """
    d = context['request'].GET.copy()
    for k, v in kwargs.items():
        d[k] = v
    for k in [k for k, v in d.items() if not v]:
        del d[k]
    return d.urlencode()


@register.filter
def wpipe_model_str(model: Union['Job', 'Event', 'Node', 'Configuration']) -> str:
    class_name = type(model).__name__
    if class_name == 'Job':
        return f'<{model.id}>'
    elif class_name == 'Event':
        return f"<{model.name} ({model.id})>"
    elif class_name == 'Node':
        string = "{}".format(model.name)
        if model.int_ip:
            string = "{} (init_ip: {})".format(string, model.init_ip)
        return string
    elif class_name == 'Configuration':
        return f"<{model.name} ({model.id})>"
    else:
        raise RuntimeError(f"This isn't a supported model: {model}")