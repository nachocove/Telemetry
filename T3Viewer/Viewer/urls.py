from django.conf.urls import patterns, url
from django.contrib import admin
from django.contrib.staticfiles.urls import staticfiles_urlpatterns

admin.autodiscover()

timestamp_regex = '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?Z|now'
userid_regex = '\w+-\w+-\d+:\w+-\w+-\w+-\w+-\w+'
deviceid_regex = 'Ncho\w+'
eventclass_regex = '\w+'
search_regex = '.*'
urlpatterns = patterns('',
    url(r'^login/$', 'Bugfix.views.login'),
    url(r'^logout/$', 'Bugfix.views.logout', name='logout'),
    url(r'^$', 'Bugfix.views.index', name='index'),
    url(r'^db/log-report-form$', 'db.views.db_log_report_form', name="log-report-form"),
    url(r'^db/log-report/(?P<project>\w+)/(?P<from_date>%s)/(?P<to_date>%s)/$' % (timestamp_regex, timestamp_regex), 'db.views.db_log_report'),
     url(r'^db/syncfail-report-form$', 'db.views.db_syncfail_report_form', name="syncfail-report-form"),
    url(r'^db/syncfail-report/(?P<project>\w+)/(?P<from_date>%s)/(?P<to_date>%s)/(?P<delta_value>\d*)/$' % (timestamp_regex, timestamp_regex), 'db.views.db_syncfail_report'),
    url(r'^db/help$', 'db.views.db_help', name="db-help"),
    url(r'^db/db-load$', 'db.views.db_load', name="db-load"),
    url(r'^db/db-delete$', 'db.views.db_delete', name="db-delete"),
    url(r'^db/db-query$', 'db.views.db_query', name="db-query"),
    url(r'^db/$', 'Bugfix.views.index', name="index"),
    url(r'^bugfix/$', 'Bugfix.views.home', name="bugfix-home"),
    url(r'^bugfix/(?P<project>\w+)/logs/(?P<event_class>%s)/(?P<userid>%s)/(?P<timestamp>%s)/(?P<span>\d+)/(?P<search>%s)/(?P<threadid>\d*)/$' % (eventclass_regex,userid_regex, timestamp_regex, search_regex), 'Bugfix.views.entry_page'),
    url(r'^bugfix/(?P<project>\w+)/logs/(?P<event_class>%s)/(?P<userid>%s)/(?P<deviceid>%s)/(?P<timestamp>%s)/(?P<span>\d+)/(?P<search>%s)/(?P<threadid>\d*)/$' % (eventclass_regex, userid_regex, deviceid_regex, timestamp_regex, search_regex), 'Bugfix.views.entry_page'),
    url(r'^bugfix/(?P<project>\w+)/logs/(?P<event_class>%s)/(?P<deviceid>%s)/(?P<timestamp>%s)/(?P<span>\d+)/(?P<search>%s)/(?P<threadid>\d*)/$' % (eventclass_regex, deviceid_regex, timestamp_regex, search_regex), 'Bugfix.views.entry_page'),
    url(r'^bugfix/(?P<project>\w+)/logs/(?P<event_class>%s)/(?P<timestamp>%s)/(?P<span>\d+)/(?P<search>%s)/(?P<threadid>\d*)/$' % (eventclass_regex, timestamp_regex, search_regex), 'Bugfix.views.entry_page'),
    url(r'^bugfix/(?P<project>\w+)/logs/(?P<event_class>%s)/(?P<userid>%s)/(?P<timestamp>%s)/(?P<span>\d+)/(?P<threadid>\d*)/$' % (eventclass_regex, userid_regex, timestamp_regex), 'Bugfix.views.entry_page'),
    url(r'^bugfix/(?P<project>\w+)/logs/(?P<event_class>%s)/(?P<userid>%s)/(?P<deviceid>%s)/(?P<timestamp>%s)/(?P<span>\d+)/(?P<threadid>\d*)/$' % (eventclass_regex, userid_regex, deviceid_regex, timestamp_regex), 'Bugfix.views.entry_page'),
    url(r'^bugfix/(?P<project>\w+)/logs/(?P<event_class>%s)/(?P<deviceid>%s)/(?P<timestamp>%s)/(?P<span>\d+)/(?P<threadid>\d*)/$' % (eventclass_regex, deviceid_regex, timestamp_regex), 'Bugfix.views.entry_page'),
    url(r'^bugfix/(?P<project>\w+)/logs/(?P<event_class>%s)/(?P<timestamp>%s)/(?P<span>\d+)/(?P<threadid>\d*)/$' % (eventclass_regex, timestamp_regex), 'Bugfix.views.entry_page'),
    url(r'^bugfix/(?P<project>\w+)/logs/(?P<event_class>%s)/(?P<userid>%s)/(?P<timestamp>%s)/(?P<span>\d+)/(?P<search>%s)/$' % (eventclass_regex,userid_regex, timestamp_regex, search_regex), 'Bugfix.views.entry_page'),
    url(r'^bugfix/(?P<project>\w+)/logs/(?P<event_class>%s)/(?P<userid>%s)/(?P<deviceid>%s)/(?P<timestamp>%s)/(?P<span>\d+)/(?P<search>%s)/$' % (eventclass_regex, userid_regex, deviceid_regex, timestamp_regex, search_regex), 'Bugfix.views.entry_page'),
    url(r'^bugfix/(?P<project>\w+)/logs/(?P<event_class>%s)/(?P<deviceid>%s)/(?P<timestamp>%s)/(?P<span>\d+)/(?P<search>%s)/$' % (eventclass_regex, deviceid_regex, timestamp_regex, search_regex), 'Bugfix.views.entry_page'),
    url(r'^bugfix/(?P<project>\w+)/logs/(?P<event_class>%s)/(?P<timestamp>%s)/(?P<span>\d+)/(?P<search>%s)/$' % (eventclass_regex, timestamp_regex, search_regex), 'Bugfix.views.entry_page'),
    url(r'^bugfix/(?P<project>\w+)/logs/(?P<event_class>%s)/(?P<userid>%s)/(?P<timestamp>%s)/(?P<span>\d+)/$' % (eventclass_regex, userid_regex, timestamp_regex), 'Bugfix.views.entry_page'),
    url(r'^bugfix/(?P<project>\w+)/logs/(?P<event_class>%s)/(?P<userid>%s)/(?P<deviceid>%s)/(?P<timestamp>%s)/(?P<span>\d+)/$' % (eventclass_regex, userid_regex, deviceid_regex, timestamp_regex), 'Bugfix.views.entry_page'),
    url(r'^bugfix/(?P<project>\w+)/logs/(?P<event_class>%s)/(?P<deviceid>%s)/(?P<timestamp>%s)/(?P<span>\d+)/$' % (eventclass_regex, deviceid_regex, timestamp_regex), 'Bugfix.views.entry_page'),
    url(r'^bugfix/(?P<project>\w+)/logs/(?P<event_class>%s)/(?P<timestamp>%s)/(?P<span>\d+)/$' % (eventclass_regex, timestamp_regex), 'Bugfix.views.entry_page'),
    # old half supported search on date range. fix before reviving
    #url(r'^bugfix/(?P<project>\w+)/logs/(?P<event_class>%s)/(?P<userid>%s)/(?P<after>%s)/(?P<before>%s)/$' % (eventclass_regex, userid_regex, timestamp_regex, timestamp_regex), 'Bugfix.views.entry_page_by_timestamps'),
)

urlpatterns += staticfiles_urlpatterns()