# Copyright 2014, NachoCove, Inc


from django.conf.urls import patterns, url
from Viewer import timestamp_regex

urlpatterns = patterns('',
    url(r'^$', 'Reports.views.home', name="reports-home"),
    url(r'^(?P<project>\w+)/emails/(?P<start>%s)/(?P<end>%s)/$' % (timestamp_regex, timestamp_regex), 'Reports.views.emails_per_timeframe'),
)
