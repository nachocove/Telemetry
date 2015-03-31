from django.conf.urls import patterns, url, include
from django.contrib import admin
from django.contrib.staticfiles.urls import staticfiles_urlpatterns
from Viewer import client_regex

admin.autodiscover()

timestamp_regex = '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?Z'
urlpatterns = patterns('',
    url(r'^login/$', 'Bugfix.views.login'),
    url(r'^logout/$', 'Bugfix.views.logout', name='logout'),
    url(r'^$', 'Bugfix.views.home'),

    url(r'^bugfix/', include("Bugfix.urls")),
    url(r'^reports/', include("Reports.urls")),

    url(r'^pinger/(?P<project>\w+)/logs/(?P<client>%s)/(?P<after>%s)/(?P<span>\d+)/$' % (client_regex, timestamp_regex), 'Bugfix.views.pinger_telemetry'),
    url(r'^pinger/(?P<project>\w+)/logs/(?P<after>%s)/(?P<span>\d+)/$' % timestamp_regex, 'Bugfix.views.pinger_telemetry'),
    url(r'^pinger/(?P<project>\w+)/logs/(?P<after>%s)/(?P<before>%s)/$' % (timestamp_regex, timestamp_regex), 'Bugfix.views.pinger_telemetry'),
)

urlpatterns += staticfiles_urlpatterns()