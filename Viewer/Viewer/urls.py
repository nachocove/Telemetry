from django.conf.urls import patterns, url
from django.contrib import admin
from django.contrib.staticfiles.urls import staticfiles_urlpatterns

admin.autodiscover()

timestamp_regex = '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?Z'
client_regex = '\w+-\w+-\d+:\w+-\w+-\w+-\w+-\w+'
urlpatterns = patterns('',
    url(r'^login/$', 'Bugfix.views.login'),
    url(r'^logout/$', 'Bugfix.views.logout', name='logout'),
    url(r'^$', 'Bugfix.views.home'),
    url(r'^bugfix/$', 'Bugfix.views.home', name="bugfix-home"),
    url(r'^bugfix/logs/(?P<client>%s)/(?P<timestamp>%s)/(?P<span>\d+)/$' % (client_regex, timestamp_regex), 'Bugfix.views.entry_page_legacy'),
    url(r'^bugfix/(?P<project>\w+)/logs/(?P<client>%s)/(?P<timestamp>%s)/(?P<span>\d+)/$' % (client_regex, timestamp_regex), 'Bugfix.views.entry_page'),
    url(r'^bugfix/(?P<project>\w+)/logs/(?P<client>%s)/(?P<after>%s)/(?P<before>%s)/$' % (client_regex, timestamp_regex, timestamp_regex), 'Bugfix.views.entry_page_by_timestamps'),
    url(r'^bugfix/search/$', 'Bugfix.views.search', name="search-form"),
    url(r'^bugfix/(?P<project>\w+)/search/(?P<after>%s)/(?P<before>%s)/$' % (timestamp_regex, timestamp_regex), 'Bugfix.views.search_results'),

    url(r'^pinger/(?P<project>\w+)/logs/(?P<client>%s)/(?P<after>%s)/(?P<span>\d+)/$' % (client_regex, timestamp_regex), 'Bugfix.views.pinger_telemetry'),
    url(r'^pinger/(?P<project>\w+)/logs/(?P<after>%s)/(?P<span>\d+)/$' % timestamp_regex, 'Bugfix.views.pinger_telemetry'),
    url(r'^pinger/(?P<project>\w+)/logs/(?P<after>%s)/(?P<before>%s)/$' % (timestamp_regex, timestamp_regex), 'Bugfix.views.pinger_telemetry'),
)

urlpatterns += staticfiles_urlpatterns()