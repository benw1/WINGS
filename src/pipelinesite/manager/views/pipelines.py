from django.shortcuts import render

from pipelinesite.models import Pipelines, Tasks, Inputs, Targets

def pipelines(request):
    all_pipes = Pipelines.objects.all()
    context = {'pipelines': all_pipes}
    return render(request, 'pipelines.html', context)

def pipeline_single(request, pk):
    # Process request
    pipe = Pipelines.objects.get(pk=pk)
    tasks = Tasks.objects.all().filter(pipeline=pipe)
    pipe_inputs = Inputs.objects.filter(pipeline_id=pipe)
    pipe_config_data_product = pipe.config_data_product
    print('PIPE INPUTS', pipe_inputs)
    all_targets = []
    for input in pipe_inputs:
        print('Input name', input.confpath)
        targets = Targets.objects.filter(input_id=input)
        all_targets.extend(targets)

    for target in all_targets:
        print("    target:", target, target.datapath, target.dataraws)

    # print("PIPELINE", pipe)
    # print("TASKS",  tasks)

    print("    DATAPRODUCTS: ", pipe.config_data_product())
    # see Target.py method configure_target(). self.input.confdataproducts

    context = {'pipeline': pipe, 'tasks': tasks, 'pipe_inputs': pipe_inputs, 'targets': all_targets}
    return render(request, 'pipeline_single.html', context)