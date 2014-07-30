from django.conf.urls import patterns, include, url

from django.contrib import admin
admin.autodiscover()

urlpatterns = patterns('',
    # Examples:
    #url(r'^$', 'Viewer.views.home', name='home'),
    #url(r'^admin/', include(admin.site.urls)),
    url(r'^login/$', 'Bugfix.views.login'),
    url(r'^$', 'Bugfix.views.home'),
    url(r'^bugfix/$', 'Bugfix.views.home'),
    url(r'^bugfix/logs/(?P<client>\w+)/(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?Z)/(?P<span>\d+)/$', 'Bugfix.views.entry_page'),
)
