from django.urls import path

from .views.pipelines import pipelines, pipeline_single
from .views.jobs import jobs, job_single_view
from .views.users import users



app_name = 'manager'
urlpatterns = [
    path('pipelines', pipelines, name='pipeline_list'),
    path('pipelines/<int:pk>', pipeline_single, name='pipeline_single'),
    path('users', users, name='users'),
    path('jobs', jobs, name='jobs'),
    path('jobs/<int:pk>', job_single_view, name='job_single_view')
]