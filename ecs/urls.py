#! /usr/bin/env python
# encoding: utf-8
"""
Copyright (C) 2017 Yunrong Technology

description：
author：yutingting
time：2017/5/4
PN: 
"""
from django.conf.urls import patterns, include, url
from django.contrib import admin
from ecs import views

urlpatterns = patterns('',
    # Examples:
    # url(r'^$', 'aliyun.views.home', name='home'),
    # url(r'^blog/', include('blog.urls')),

    url(r'^$', views.index, name='index'),
)
