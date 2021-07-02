# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey and OneToOneField has `on_delete` set to the desired behavior
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from django.db import models


class Tasks(models.Model):
    name = models.CharField(max_length=256, blank=True, null=True)
    timestamp = models.DateTimeField(blank=True, null=True)
    nruns = models.IntegerField(blank=True, null=True)
    run_time = models.FloatField(blank=True, null=True)
    is_exclusive = models.IntegerField(blank=True, null=True)
    pipeline = models.ForeignKey('Pipelines', models.DO_NOTHING, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'tasks'
