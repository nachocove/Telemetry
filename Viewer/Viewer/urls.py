from django.conf.urls import patterns, url
from django.contrib import admin
from django.contrib.staticfiles.urls import staticfiles_urlpatterns

admin.autodiscover()

timestamp_regex = '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?Z'
urlpatterns = patterns('',
    # Examples:
    #url(r'^$', 'Viewer.views.home', name='home'),
    #url(r'^admin/', include(admin.site.urls)),
    url(r'^login/$', 'Bugfix.views.login'),
    url(r'^logout/$', 'Bugfix.views.logout', name='logout'),
    url(r'^$', 'Bugfix.views.home'),
    url(r'^bugfix/$', 'Bugfix.views.home', name="bugfix-home"),
    url(r'^bugfix/logs/(?P<client>\w+-\w+-\d+:\w+-\w+-\w+-\w+-\w+)/(?P<timestamp>%s)/(?P<span>\d+)/$' % timestamp_regex, 'Bugfix.views.entry_page_legacy'),
    url(r'^bugfix/(?P<project>\w+)/logs/(?P<client>\w+-\w+-\d+:\w+-\w+-\w+-\w+-\w+)/(?P<timestamp>%s)/(?P<span>\d+)/$' % timestamp_regex, 'Bugfix.views.entry_page'),
    url(r'^bugfix/(?P<project>\w+)/logs/(?P<client>\w+-\w+-\d+:\w+-\w+-\w+-\w+-\w+)/(?P<after>%s)/(?P<before>%s)/$' % (timestamp_regex, timestamp_regex), 'Bugfix.views.entry_page_by_timestamps'),
)

urlpatterns += staticfiles_urlpatterns()