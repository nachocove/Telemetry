# Copyright 2014, NachoCove, Inc


from django.conf.urls import patterns, url
from Viewer import timestamp_regex

urlpatterns = patterns('',
    url(r'^$', 'Reports.views.home', name="reports-home"),
    url(r'^(?P<project>\w+)/monitor-reports/(?P<report>[^/]+)/(?P<start>%s)/(?P<end>%s)/$' % (timestamp_regex, timestamp_regex), 'Reports.views.monitor_reports'),
)
