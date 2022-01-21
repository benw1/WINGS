from django.urls import path

from .views.pipelines import pipelines
from .views.pipelines import pipeline_single
from .views.users import users
from .views.jobs import jobs


app_name = 'manager'
urlpatterns = [
    path('pipelines', pipelines, name='pipeline_list'),
    path('pipelines/<int:pk>', pipeline_single, name='pipeline_single'),
    path('users', users, name='users'),
    path('jobs', jobs, name='jobs')
]