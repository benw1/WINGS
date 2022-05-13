# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey and OneToOneField has `on_delete` set to the desired behavior
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from django.db import models


class Jobs(models.Model):
    id = models.OneToOneField('Optowners', models.DO_NOTHING, db_column='id', primary_key=True)
    attempt = models.IntegerField(blank=True, null=True)
    state = models.CharField(max_length=256, blank=True, null=True)
    starttime = models.DateTimeField(blank=True, null=True)
    endtime = models.DateTimeField(blank=True, null=True)
    node = models.ForeignKey('Nodes', models.DO_NOTHING, blank=True, null=True)
    config = models.ForeignKey('Configurations', models.DO_NOTHING, blank=True, null=True)
    task = models.ForeignKey('Tasks', models.DO_NOTHING, blank=True, null=True)
    firing_event = models.ForeignKey('Events', models.DO_NOTHING, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'jobs'

    def __str__(self):
        return str(id)