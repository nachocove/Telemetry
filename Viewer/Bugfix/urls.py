# Copyright 2014, NachoCove, Inc


from django.conf.urls import patterns, url
from Viewer import timestamp_regex, client_regex

urlpatterns = patterns('',
    url(r'^$', 'Bugfix.views.home', name="bugfix-home"),
    url(r'^logs/(?P<client>%s)/(?P<timestamp>%s)/(?P<span>\d+)/$' % (client_regex, timestamp_regex), 'Bugfix.views.entry_page_legacy'),
    url(r'^(?P<project>\w+)/logs/(?P<client>%s)/(?P<timestamp>%s)/(?P<span>\d+)/$' % (client_regex, timestamp_regex), 'Bugfix.views.entry_page'),
    url(r'^(?P<project>\w+)/logs/(?P<client>%s)/(?P<after>%s)/(?P<before>%s)/$' % (client_regex, timestamp_regex, timestamp_regex), 'Bugfix.views.entry_page_by_timestamps'),
    url(r'^search/$', 'Bugfix.views.search', name="search-form"),
    url(r'^(?P<project>\w+)/search/(?P<after>%s)/(?P<before>%s)/$' % (timestamp_regex, timestamp_regex), 'Bugfix.views.search_results'),
)
