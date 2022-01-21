# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey and OneToOneField has `on_delete` set to the desired behavior
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from django.db import models


class Configurations(models.Model):
    id = models.OneToOneField('Dpowners', models.DO_NOTHING, db_column='id', primary_key=True)
    name = models.CharField(max_length=256, blank=True, null=True)
    datapath = models.CharField(max_length=256, blank=True, null=True)
    confpath = models.CharField(max_length=256, blank=True, null=True)
    rawpath = models.CharField(max_length=256, blank=True, null=True)
    logpath = models.CharField(max_length=256, blank=True, null=True)
    procpath = models.CharField(max_length=256, blank=True, null=True)
    description = models.CharField(max_length=256, blank=True, null=True)
    target = models.ForeignKey('Targets', models.DO_NOTHING, blank=True, null=True)

    def __str__(self):
        return "<{} ({})>".format(self.name, self.id)

    class Meta:
        managed = False
        db_table = 'configurations'
