from django.conf.urls import patterns, url
from django.contrib import admin
from django.contrib.staticfiles.urls import staticfiles_urlpatterns

admin.autodiscover()

timestamp_regex = '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?Z'
userid_regex = '\w+-\w+-\d+:\w+-\w+-\w+-\w+-\w+'
deviceid_regex = 'Ncho\w+'
eventclass_regex = '\w+'
urlpatterns = patterns('',
    url(r'^login/$', 'Bugfix.views.login'),
    url(r'^logout/$', 'Bugfix.views.logout', name='logout'),
    url(r'^$', 'Bugfix.views.home'),
    url(r'^bugfix/$', 'Bugfix.views.home', name="bugfix-home"),
    url(r'^bugfix/(?P<project>\w+)/logs/(?P<event_class>%s)/(?P<userid>%s)/(?P<timestamp>%s)/(?P<span>\d+)/$' % (eventclass_regex,userid_regex, timestamp_regex), 'Bugfix.views.entry_page'),
    url(r'^bugfix/(?P<project>\w+)/logs/(?P<event_class>%s)/(?P<userid>%s)/(?P<deviceid>%s)/(?P<timestamp>%s)/(?P<span>\d+)/$' % (eventclass_regex, userid_regex, deviceid_regex, timestamp_regex), 'Bugfix.views.entry_page'),
    url(r'^bugfix/(?P<project>\w+)/logs/(?P<event_class>%s)/(?P<deviceid>%s)/(?P<timestamp>%s)/(?P<span>\d+)/$' % (eventclass_regex, deviceid_regex, timestamp_regex), 'Bugfix.views.entry_page'),
    url(r'^bugfix/(?P<project>\w+)/logs/(?P<event_class>%s)/(?P<userid>%s)/(?P<timestamp>%s)/(?P<span>\d+)/$' % (eventclass_regex, userid_regex, timestamp_regex), 'Bugfix.views.entry_page'),
    url(r'^bugfix/(?P<project>\w+)/logs/(?P<event_class>%s)/(?P<timestamp>%s)/(?P<span>\d+)/$' % (eventclass_regex, timestamp_regex), 'Bugfix.views.entry_page'),

    url(r'^bugfix/(?P<project>\w+)/logs/(?P<event_class>%s)/(?P<userid>%s)/(?P<after>%s)/(?P<before>%s)/$' % (eventclass_regex, userid_regex, timestamp_regex, timestamp_regex), 'Bugfix.views.entry_page_by_timestamps'),
)

urlpatterns += staticfiles_urlpatterns()