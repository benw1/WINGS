# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey and OneToOneField has `on_delete` set to the desired behavior
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from django.db import models


class Dataproducts(models.Model):
    id = models.OneToOneField('Optowners', models.DO_NOTHING, db_column='id', primary_key=True)
    filename = models.CharField(max_length=256, blank=True, null=True)
    relativepath = models.CharField(max_length=256, blank=True, null=True)
    suffix = models.CharField(max_length=256, blank=True, null=True)
    data_type = models.CharField(max_length=256, blank=True, null=True)
    subtype = models.CharField(max_length=256, blank=True, null=True)
    group = models.CharField(max_length=256, blank=True, null=True)
    filtername = models.CharField(max_length=256, blank=True, null=True)
    ra = models.FloatField(blank=True, null=True)
    dec = models.FloatField(blank=True, null=True)
    pointing_angle = models.FloatField(blank=True, null=True)
    dpowner = models.ForeignKey('Dpowners', models.DO_NOTHING, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'dataproducts'
