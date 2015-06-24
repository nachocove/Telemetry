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
from AWS.db_reports import log_report, execute_sql
from core.auth import nacho_cache, nachotoken_required

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

class DBDeleteForm(forms.Form):
    project = forms.ChoiceField(choices=[(x, x.capitalize()) for x in projects])
    from_date = forms.DateTimeField(initial=datetime.now())
    to_date = forms.DateTimeField(initial=datetime.now())
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
        if from_date.replace(tzinfo=None) > datetime.now():
            self.add_error('from_date', 'Date/Time in future')
            raise ValidationError(_('Date/Time in future: %(from_date)s'))
        return from_date

    def clean_to_date(self):
        to_date = self.cleaned_data.get('to_date', '')
        if to_date.replace(tzinfo=None) > datetime.now():
            self.add_error('to_date', 'Date/Time in future')
            raise ValidationError(_('Date/Time in future: %(to_date)s'))
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
        cleaned_data = super(DBDeleteForm, self).clean()
        from_date = cleaned_data.get("from_date")
        to_date = cleaned_data.get("to_date")
        if to_date < from_date:
            raise forms.ValidationError("FromDate(%s) cannot be > ToDate(%s)" % (from_date, to_date))
        elif (to_date-from_date).days > 31:
            raise forms.ValidationError("FromDate(%s)-ToDate(%s) cannot be > 31 days" % (from_date, to_date))

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
        if to_date < from_date:
            raise forms.ValidationError("FromDate(%s) cannot be > ToDate(%s)" % (from_date, to_date))
        elif (to_date-from_date).days > 31:
            raise forms.ValidationError("FromDate(%s)-ToDate(%s) cannot be > 31 days" % (from_date, to_date))


class DBReportForm(forms.Form):
    project = forms.ChoiceField(choices=[(x, x.capitalize()) for x in projects])
    from_date = forms.DateTimeField(initial=datetime.now())
    to_date = forms.DateTimeField(initial=datetime.now())

    def clean_from_date(self):
        from_date = self.cleaned_data.get('from_date', '')
        if from_date.replace(tzinfo=None) > datetime.now():
            self.add_error('from_date', 'Date/Time in future')
            raise ValidationError(_('Date/Time in future: %(from_date)s'))
        return from_date

    def clean_to_date(self):
        to_date = self.cleaned_data.get('to_date', '')
        if to_date.replace(tzinfo=None) > datetime.now():
            self.add_error('to_date', 'Date/Time in future')
            raise ValidationError(_('Date/Time in future: %(to_date)s'))
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
        cleaned_data = super(DBReportForm, self).clean()
        from_date = cleaned_data.get("from_date")
        to_date = cleaned_data.get("to_date")
        if to_date < from_date:
            raise forms.ValidationError("FromDate(%s) cannot be > ToDate(%s)" % (from_date, to_date))
        elif (to_date-from_date).days > 31:
            raise forms.ValidationError("FromDate(%s)-ToDate(%s) cannot be > 31 days" % (from_date, to_date))

class DBQueryForm(forms.Form):
    project = forms.ChoiceField(choices=[(x, x.capitalize()) for x in projects])
    sql_query = forms.CharField(widget=forms.Textarea)

    def clean_project(self):
        project = self.cleaned_data.get('project', '')
        if project not in projects:
            self.add_error('project', 'Unknown Project')
            raise ValidationError(_('Unknown Project: %(project)s'),
                                  code='unknown',
                                  params={'project': projects})
        return project

# Create your views here.
@nachotoken_required
def db_load(request):
    logger = logging.getLogger('telemetry').getChild('db')
    # Any message set in 'message' will be displayed as a red error message.
    # Used for reporting error in any POST.
    message = ''
    if request.method != 'POST':
        form = DBLoadForm()
        form.fields['project'].initial = request.session.get('project', default_project)
        return render_to_response('db_load.html', {'form': form, 'message': message},
                                  context_instance=RequestContext(request))

    form = DBLoadForm(request.POST)
    if not form.is_valid():
        logger.warn('invalid form data')
        return render_to_response('db_load.html', {'form': form, 'message': message},
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
        summary["event_classes"] = event_classes
        for ev_class in event_classes:
            if "table_name" in summary:
                summary["table_name"] = summary["table_name"] + ", " + \
                                    table_prefix + "_" + project + \
                                    "_nm_" + T3_EVENT_CLASS_FILE_PREFIXES[ev_class]
            else:
                summary["table_name"] = table_prefix + "_" + project + \
                                    "_nm_" + T3_EVENT_CLASS_FILE_PREFIXES[ev_class]
    else:
        summary["event_classes"] = event_class
        summary["table_name"] = table_prefix + "_" + project + "_nm_" + T3_EVENT_CLASS_FILE_PREFIXES[event_class]
    t3_redshift_config = get_t3_redshift_config(project, projects_cfg.get(project, 'report_config_file'))
    status = create_tables(logger, project, t3_redshift_config, event_class, table_prefix)
    upload_stats = upload_logs(logger, project, t3_redshift_config, event_class, from_datetime, to_datetime, table_prefix)
    report_data = {'summary': summary, 'upload_stats': upload_stats, "general_config": t3_redshift_config["general_config"]}
    return render_to_response('upload_report.html', report_data,
                              context_instance=RequestContext(request))

@nachotoken_required
def db_delete(request):
    logger = logging.getLogger('telemetry').getChild('db')
    message = ''
    if request.method != 'POST':
        form = DBDeleteForm()
        form.fields['project'].initial = request.session.get('project', default_project)
        return render_to_response('db_delete.html', {'form': form, 'message': message},
                                  context_instance=RequestContext(request))

    form = DBDeleteForm(request.POST)
    if not form.is_valid():
        logger.warn('invalid form data')
        return render_to_response('db_delete.html', {'form': form, 'message': message},
                                  context_instance=RequestContext(request))
    project = form.cleaned_data['project']
    from_date = form.cleaned_data['from_date']
    to_date = form.cleaned_data['to_date']
    event_class = form.cleaned_data['event_class']
    table_prefix = form.cleaned_data['table_prefix']
    logger.debug("Deleting data from the database Project:%s, "
                 "From Date:%s, To Date:%s, Event Class=%s, Table Prefix:%s",
                 project, from_date, to_date, event_class, table_prefix)

    request.session['project'] = project
    request.session['event_class'] = event_class
    from_datetime = UtcDateTime(from_date)
    to_datetime = UtcDateTime(to_date)
    summary = {}
    summary["start"] = from_datetime
    summary["end"] =  to_datetime
    event_classes = T3_EVENT_CLASS_FILE_PREFIXES[event_class]
    if isinstance(event_classes, list):
        summary["event_classes"] = event_classes
        for ev_class in event_classes:
            if "table_name" in summary:
                summary["table_name"] = summary["table_name"] + ", " + \
                                    table_prefix + "_" + project + \
                                    "_nm_" + T3_EVENT_CLASS_FILE_PREFIXES[ev_class]
            else:
                summary["table_name"] = table_prefix + "_" + project + \
                                    "_nm_" + T3_EVENT_CLASS_FILE_PREFIXES[ev_class]
    else:
        summary["event_classes"] = event_class
        summary["table_name"] = table_prefix + "_" + project + "_nm_" + T3_EVENT_CLASS_FILE_PREFIXES[event_class]
    t3_redshift_config = get_t3_redshift_config(project, projects_cfg.get(project, 'report_config_file'))
    delete_stats = delete_logs(logger, project, t3_redshift_config, event_class, from_datetime, to_datetime, table_prefix)
    report_data = {'summary': summary, 'delete_stats': delete_stats,
                   "general_config": t3_redshift_config["general_config"], 'message':message}
    return render_to_response('delete_report.html', report_data,
                              context_instance=RequestContext(request))

@nachotoken_required
def db_log_report_form(request):
    logger = logging.getLogger('telemetry').getChild('db')
    # Any message set in 'message' will be displayed as a red error message.
    # Used for reporting error in any POST.
    message = ''
    if request.method != 'POST':
        form = DBReportForm()
        form.fields['project'].initial = request.session.get('project', default_project)
        return render_to_response('log_form.html', {'form': form, 'message': message},
                                  context_instance=RequestContext(request))

    form = DBReportForm(request.POST)
    if not form.is_valid():
        logger.warn('invalid form data')
        return render_to_response('log_form.html', {'form': form, 'message': message},
                                  context_instance=RequestContext(request))
    kwargs={}
    kwargs['project'] = form.cleaned_data['project']
    kwargs['from_date'] = UtcDateTime(form.cleaned_data['from_date'])
    kwargs['to_date'] = UtcDateTime(form.cleaned_data['to_date'])
    return HttpResponseRedirect(reverse(db_log_report, kwargs=kwargs))

def db_log_report(request, project, from_date, to_date):
    logger = logging.getLogger('telemetry').getChild('db')
    logger.debug("Running log report for Project:%s, "
                 "From Date:%s, To Date:%s",
                 project, from_date, to_date)
    request.session['project'] = project
    from_datetime = UtcDateTime(from_date)
    to_datetime = UtcDateTime(to_date)
    summary = {}
    summary["start"] = from_datetime
    summary["end"] =  to_datetime
    t3_redshift_config = get_t3_redshift_config(project, projects_cfg.get(project, 'report_config_file'))
    error_list = []
    warning_list = []
    summary, error_list, warning_list = log_report(logger, t3_redshift_config['general_config']['project'],
                                                   t3_redshift_config, from_datetime, to_datetime)
    report_data = {'summary': summary, 'errors': error_list, 'warnings': warning_list, "general_config": t3_redshift_config["general_config"] }
    return render_to_response('log_report.html', report_data,
                              context_instance=RequestContext(request))

@nachotoken_required
def db_help(request):
    logger = logging.getLogger('telemetry').getChild('db')
    # Any message set in 'message' will be displayed as a red error message.
    # Used for reporting error in any POST.
    message = ''
    return render_to_response('help.html', {'message': message},
                                  context_instance=RequestContext(request))

@nachotoken_required
def db_query(request):
    logger = logging.getLogger('telemetry').getChild('db')
    message = ''
    if request.method != 'POST':
        form = DBQueryForm()
        return render_to_response('db_query.html', {'form': form, 'message': message},
                                  context_instance=RequestContext(request))
    form = DBQueryForm(request.POST)
    if not form.is_valid():
        logger.warn('invalid form data')
        return render_to_response('db_query.html', {'form': form, 'message': message},
                                  context_instance=RequestContext(request))
    sql_query = form.cleaned_data['sql_query']
    project = form.cleaned_data['project']
    t3_redshift_config = get_t3_redshift_config(project, projects_cfg.get(project, 'report_config_file'))
    error, col_names, results = execute_sql(logger, project, t3_redshift_config, sql_query)
    report_data = {'results': results, 'col_names': col_names, 'message':error, "general_config": t3_redshift_config["general_config"], 'form': form}
    return render_to_response('db_query.html', report_data,
                              context_instance=RequestContext(request))