from django.urls import path

from .views.users import users
from .views.pipelines import pipelines, pipeline_single
from .views.jobs import jobs, job_single_view
from .views.tasks import task_list, task_detail_view
from .views.events import event_list, event_detail_view



app_name = 'manager'
urlpatterns = [
    path('users', users, name='users'),

    path('pipelines', pipelines, name='pipeline_list'),
    path('pipelines/<int:pk>', pipeline_single, name='pipeline_single'),

    path('jobs', jobs, name='jobs'),
    path('jobs/<int:pk>', job_single_view, name='job_single_view'),

    path('tasks', task_list, name='tasks'),
    path('tasks/<int:pk>', task_detail_view, name='task_detail_view'),


    path('events', event_list, name='events'),
    path('events/<int:pk>', event_detail_view, name='event_detail_view'),
]