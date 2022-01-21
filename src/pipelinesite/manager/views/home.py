from django.shortcuts import render
from django.shortcuts import HttpResponse

# def home(request):
#     return HttpResponse('{% extends "base.html" %}{% load static %}<h1> This is the WINGS PIPELINE Home Page </h1>')

def home(request):
    return render(request, 'home.html')