from django.shortcuts import render

from pipelinesite.models import Users

def users(request):
    all_users = Users.objects.all()
    context = {'users': all_users}
    return render(request, 'users.html', context)