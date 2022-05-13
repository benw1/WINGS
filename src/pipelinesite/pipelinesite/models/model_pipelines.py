# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey and OneToOneField has `on_delete` set to the desired behavior
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from django.db import models
import os

from pipelinesite.models import Dataproducts
from pipelinesite.utils import DataProductGroups

class Pipelines(models.Model):
    id = models.OneToOneField('Dpowners', models.DO_NOTHING, db_column='id', primary_key=True)
    name = models.CharField(max_length=256, blank=True, null=True)
    pipe_root = models.CharField(max_length=256, blank=True, null=True)
    software_root = models.CharField(max_length=256, blank=True, null=True)
    input_root = models.CharField(max_length=256, blank=True, null=True)
    data_root = models.CharField(max_length=256, blank=True, null=True)
    config_root = models.CharField(max_length=256, blank=True, null=True)
    description = models.CharField(max_length=256, blank=True, null=True)
    user = models.ForeignKey('Users', models.DO_NOTHING, blank=True, null=True)


    def __str__(self):
        return f'<{self.name} ({self.id})>'

    class Meta:
        managed = False
        db_table = 'pipelines'

    def config_data_product(self) -> Dataproducts:
        return Dataproducts.objects.get(dpowner=self.id, group=DataProductGroups.CONFIGURATION.value)

    def config_data_product_path(self) -> str:
        config_data_product = self.config_data_product()
        return os.path.join(config_data_product.relativepath, config_data_product.filename)

