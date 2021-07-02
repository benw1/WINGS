from django.contrib import admin
# from myproject.myapp.models import Author
from pipelinesite.models.model_configurations import Configurations
from pipelinesite.models.model_dataproducts import Dataproducts
from pipelinesite.models.model_dpowners import Dpowners
from pipelinesite.models.model_events import Events
from pipelinesite.models.model_inputs import Inputs
from pipelinesite.models.model_jobs import Jobs
from pipelinesite.models.model_masks import Masks
from pipelinesite.models.model_nodes import Nodes
from pipelinesite.models.model_options import Options
from pipelinesite.models.model_optowners import Optowners
from pipelinesite.models.model_parameters import Parameters
from pipelinesite.models.model_pipelines import Pipelines
from pipelinesite.models.model_targets import Targets
from pipelinesite.models.model_tasks import Tasks
from pipelinesite.models.model_users import Users

admin.site.register(Configurations)
admin.site.register(Dataproducts)
admin.site.register(Dpowners)
admin.site.register(Events)
admin.site.register(Inputs)
admin.site.register(Jobs)
admin.site.register(Masks)
admin.site.register(Nodes)
admin.site.register(Options)
admin.site.register(Optowners)
admin.site.register(Parameters)
admin.site.register(Pipelines)
admin.site.register(Targets)
admin.site.register(Tasks)
admin.site.register(Users)