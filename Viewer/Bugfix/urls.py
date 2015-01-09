# Copyright 2014, NachoCove, Inc


from django.conf.urls import patterns, url
from Viewer import timestamp_regex

urlpatterns = patterns('',
    url(r'^$', 'Bugfix.views.home', name="bugfix-home"),
    url(r'^logs/(?P<client>\w+-\w+-\d+:\w+-\w+-\w+-\w+-\w+)/(?P<timestamp>%s)/(?P<span>\d+)/$' % timestamp_regex, 'Bugfix.views.entry_page_legacy'),
    url(r'^(?P<project>\w+)/logs/(?P<client>\w+-\w+-\d+:\w+-\w+-\w+-\w+-\w+)/(?P<timestamp>%s)/(?P<span>\d+)/$' % timestamp_regex, 'Bugfix.views.entry_page'),
    url(r'^(?P<project>\w+)/logs/(?P<client>\w+-\w+-\d+:\w+-\w+-\w+-\w+-\w+)/(?P<after>%s)/(?P<before>%s)/$' % (timestamp_regex, timestamp_regex), 'Bugfix.views.entry_page_by_timestamps'),
)
