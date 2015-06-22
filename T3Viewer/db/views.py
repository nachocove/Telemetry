import base64
from functools import wraps
from gettext import gettext as _
import hashlib
import os
from datetime import timedelta, datetime, date
import logging
import cgi
import json
import ConfigParser
from urllib import urlencode

import dateutil.parser
from django.conf import settings
from django.core.exceptions import ValidationError
from django.core.urlresolvers import reverse
from django.http import HttpResponseBadRequest
from django import forms
from django.shortcuts import render, render_to_response
from django.http import HttpResponseRedirect
from django.template import RequestContext
from django.utils.decorators import available_attrs
from django.views.decorators.cache import cache_control
from django.views.decorators.vary import vary_on_cookie
from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.s3.connection import S3Connection
from AWS.s3t3_telemetry import T3_EVENT_CLASS_FILE_PREFIXES
from misc.utc_datetime import UtcDateTime
from AWS.redshift_handler import delete_logs, upload_logs, create_tables

# Get the list of project
projects_cfg = ConfigParser.ConfigParser()
projects_cfg.read('projects.cfg')
projects = projects_cfg.sections()
if not projects:
    raise ValueError('No projects defined')
default_project = os.environ.get('PROJECT', projects[0])

tmp_logger = logging.getLogger('telemetry')

default_span = 1

_t3_redshift_config_cache = {}
# load json config
def get_t3_redshift_config(project, file_name):
    global _t3_redshift_config_cache
    if not project in _t3_redshift_config_cache:
        if not project in projects:
            raise ValueError('Project %s is not present in projects.cfg' % project)
        with open(file_name) as data_file:
            _t3_redshift_config_cache[project] = json.load(data_file)
        return _t3_redshift_config_cache[project]

class DBLoadForm(forms.Form):
    project = forms.ChoiceField(choices=[(x, x.capitalize()) for x in projects])
    from_date = forms.DateField(initial=date.today())
    to_date = forms.DateField(initial=date.today())
    event_class = forms.ChoiceField(choices=[(x, x.capitalize()) for x in sorted(T3_EVENT_CLASS_FILE_PREFIXES)])
    table_prefix = forms.CharField(widget=forms.TextInput, required=True)

    def clean_event_class(self):
        event_class = self.cleaned_data.get('event_class', '')
        if event_class not in T3_EVENT_CLASS_FILE_PREFIXES.keys():
            self.add_error('event_class', 'Unknown Event Class')
            raise ValidationError(_('Unknown Event Class: %(event_class)s'),
                                  code='unknown',
                                  params={'event_class': T3_EVENT_CLASS_FILE_PREFIXES.keys()})
        return event_class

    def clean_from_date(self):
        from_date = self.cleaned_data.get('from_date', '')
        if from_date > date.today():
            self.add_error('from_date', 'Date in future')
            raise ValidationError(_('Date in future: %(from_date)s'))
        return from_date

    def clean_to_date(self):
        to_date = self.cleaned_data.get('to_date', '')
        if to_date > date.today():
            self.add_error('to_date', 'Date in future')
            raise ValidationError(_('Date in future: %(to_date)s'))
        return to_date

    def clean_project(self):
        project = self.cleaned_data.get('project', '')
        if project not in projects:
            self.add_error('project', 'Unknown Project')
            raise ValidationError(_('Unknown Project: %(project)s'),
                                  code='unknown',
                                  params={'project': projects})
        return project
    def clean(self):
        cleaned_data = super(DBLoadForm, self).clean()
        from_date = cleaned_data.get("from_date")
        to_date = cleaned_data.get("to_date")
        print to_date, from_date
        if to_date < from_date:
            raise forms.ValidationError("FromDate(%s) cannot be > ToDate(%s)" % (from_date, to_date))
        elif (to_date-from_date).days > 31:
            raise forms.ValidationError("FromDate(%s)-ToDate(%s) cannot be > 31 days" % (from_date, to_date))
BOTO_DEBUG=False

_aws_s3_connection_cache = {}
def _aws_s3_connection(project):
    """
    :return: boto.s3.connection.S3Connection
    """
    global _aws_s3_connection_cache
    if not project in _aws_s3_connection_cache:
        if not project in projects:
            raise ValueError('Project %s is not present in projects.cfg' % project)
        _aws_s3_connection_cache[project] = S3Connection(host='s3-us-west-2.amazonaws.com',
                                                         port=443,
                                                         aws_secret_access_key=projects_cfg.get(project, 'secret_access_key'),
                                                         aws_access_key_id=projects_cfg.get(project, 'access_key_id'),
                                                         is_secure=True,
                                                         debug=2 if BOTO_DEBUG else 0)
    return _aws_s3_connection_cache[project]

def _parse_junk(junk, mapping):
    retval = dict()
    lines = junk.splitlines()
    for line in lines:
        splitty = line.split(':', 1)
        if 2 != len(splitty):
            continue
        key = splitty[0].strip()
        value = splitty[1].strip()
        if key in mapping:
            if mapping[key] == 'timestamp' and value.strip() == 'now':
                value = _iso_z_format(datetime.utcnow())
            retval[mapping[key]] = value.strip()
    logger = logging.getLogger('telemetry').getChild('_parse_junk')
    logger.debug('retval=%s', retval)
    return retval

def nacho_token():
    token = "NACHO:%s" % settings.SECRET_KEY
    return hashlib.sha256(token).hexdigest()

def create_session(request):
    request.session['nachotoken'] = nacho_token()

def validate_session(request):
    return request.session.get('nachotoken', None) == nacho_token()

def nachotoken_required(view_func):
    @wraps(view_func, assigned=available_attrs(view_func))
    def _wrapped_view(request, *args, **kwargs):
        return view_func(request, *args, **kwargs)
        # if validate_session(request):
        #     return view_func(request, *args, **kwargs)
        # else:
        #     return HttpResponseRedirect(settings.LOGIN_URL)
    return _wrapped_view

def nacho_cache(view_func):
    """
    A convenient function where to adjust cache settings for all cached pages. If we later
    want to add 304 processing or server-side caching, just add it here.
    """
    @wraps(view_func, assigned=available_attrs(view_func))
    @cache_control(private=True, must_revalidate=True, proxy_revalidate=True, max_age=3600)
    @vary_on_cookie
    def _wrapped_view(request, *args, **kwargs):
        return view_func(request, *args, **kwargs)
    return _wrapped_view

# Create your views here.
@nachotoken_required
def dbload(request):
    logger = logging.getLogger('telemetry').getChild('home')
    # Any message set in 'message' will be displayed as a red error message.
    # Used for reporting error in any POST.
    message = ''
    if request.method != 'POST':
        form = DBLoadForm()
        form.fields['project'].initial = request.session.get('project', default_project)
        return render_to_response('dbload.html', {'form': form, 'message': message},
                                  context_instance=RequestContext(request))

    form = DBLoadForm(request.POST)
    if not form.is_valid():
        logger.warn('invalid form data')
        return render_to_response('dbload.html', {'form': form, 'message': message},
                                  context_instance=RequestContext(request))
    project = form.cleaned_data['project']
    from_date = form.cleaned_data['from_date']
    to_date = form.cleaned_data['to_date']
    event_class = form.cleaned_data['event_class']
    table_prefix = form.cleaned_data['table_prefix']
    logger.debug("Loading Database from S3 for Project:%s, "
                 "From Date:%s, To Date:%s, Event Class=%s, Table Prefix:%s",
                 project, from_date, to_date, event_class, table_prefix)

    request.session['project'] = project
    request.session['event_class'] = event_class
    from_datetime = UtcDateTime(datetime.combine(from_date, datetime.min.time()))
    to_datetime = UtcDateTime(datetime.combine(to_date, datetime.min.time()))
    summary = {}
    summary["start"] = from_datetime
    summary["end"] =  to_datetime
    event_classes = T3_EVENT_CLASS_FILE_PREFIXES[event_class]
    if isinstance(event_classes, list):
        summary["event_types"] = event_classes
    else:
        summary["event_types"] = event_class
    summary["table_name"] = table_prefix + "_nm_" + T3_EVENT_CLASS_FILE_PREFIXES[event_class]
    t3_redshift_config = get_t3_redshift_config(project, projects_cfg.get(project, 'report_config_file'))
    status = create_tables(logger, project, t3_redshift_config, event_class, table_prefix)
    upload_stats = upload_logs(logger, project, t3_redshift_config, event_class, from_datetime, to_datetime, table_prefix)
    report_data = {'summary': summary, 'upload_stats': upload_stats, "general_config": t3_redshift_config["general_config"]}
    return render_to_response('uploadreport.html', report_data,
                              context_instance=RequestContext(request))

def _iso_z_format(date):
    raw = date.isoformat()
    keep = raw.split('+', 1)[0]
    if date.microsecond == 0:
        return keep + '.000Z'
    return keep[:-3] + 'Z'

def json_formatter(obj):
    if isinstance(obj, UtcDateTime):
        return obj.datetime.isoformat('T')
    elif isinstance(obj, datetime):
        return obj.isoformat('T')
    else:
        try:
            return str(obj)
        except Exception as e:
            raise TypeError, 'Object of type %s with value of %s not converted to string: %s' % (type(obj), repr(obj), e)
