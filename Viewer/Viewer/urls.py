from django.conf.urls import patterns, url, include
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
    url(r'^bugfix/', include("Bugfix.urls")),
    url(r'^reports/', include("Reports.urls")),
)

urlpatterns += staticfiles_urlpatterns()