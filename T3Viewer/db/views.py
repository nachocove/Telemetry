import ConfigParser
import json
import logging
import os
from datetime import timedelta, datetime, date
from gettext import gettext as _

from django import forms
from django.core.exceptions import ValidationError
from django.core.urlresolvers import reverse
from django.http import HttpResponseRedirect
from django.shortcuts import render_to_response
from django.template import RequestContext

from AWS.db_reports import log_report, execute_sql, classify_log_events, syncfail_report
from AWS.redshift_handler import delete_logs, upload_logs, create_tables, list_tables, delete_tables
from AWS.s3t3_telemetry import T3_EVENT_CLASS_FILE_PREFIXES
from Bugfix.views import entry_page
from core.auth import nachotoken_required
from misc.utc_datetime import UtcDateTime

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


class DBDeleteLogsForm(forms.Form):
    project = forms.ChoiceField(choices=[(x, x.capitalize()) for x in projects])
    from_date = forms.DateTimeField(initial=datetime.now(), required=False)
    to_date = forms.DateTimeField(initial=datetime.now(), required=False)
    event_class = forms.ChoiceField(choices=[(x, x.capitalize()) for x in sorted(T3_EVENT_CLASS_FILE_PREFIXES)])
    table_prefix = forms.CharField(widget=forms.TextInput, required=False)
    base_table = forms.BooleanField(required=False, help_text="Use the base project table, instead of a prefix.")

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
        if from_date:
            if from_date.replace(tzinfo=None) > datetime.now():
                raise ValidationError(_('Date/Time in future: %s' % from_date))
        return from_date

    def clean_to_date(self):
        to_date = self.cleaned_data.get('to_date', '')
        if to_date:
            if to_date.replace(tzinfo=None) > datetime.now():
                raise ValidationError(_('Date/Time in future: %s' % to_date))
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
        cleaned_data = super(DBDeleteLogsForm, self).clean()
        from_date = cleaned_data.get("from_date")
        to_date = cleaned_data.get("to_date")
        if to_date == None or from_date == None:
            return
        if to_date < from_date:
            raise forms.ValidationError("FromDate(%s) cannot be > ToDate(%s)" % (from_date, to_date))
        elif (to_date - from_date).days > 31:
            raise forms.ValidationError("FromDate(%s)-ToDate(%s) cannot be > 31 days" % (from_date, to_date))
        if not cleaned_data.get('table_prefix', None) and not cleaned_data.get('base_table', False):
            raise forms.ValidationError("Must select either 'Base Table' or give a table prefix")


class DBLoadForm(forms.Form):
    project = forms.ChoiceField(choices=[(x, x.capitalize()) for x in projects])
    from_date = forms.DateField(initial=date.today())
    to_date = forms.DateField(initial=date.today())
    event_class = forms.ChoiceField(choices=[(x, x.capitalize()) for x in sorted(T3_EVENT_CLASS_FILE_PREFIXES)])
    table_prefix = forms.CharField(widget=forms.TextInput, required=False)
    reload_data = forms.BooleanField(required=False, help_text="Delete the data in the given timespan before upload to avoid duplication.")
    base_table = forms.BooleanField(required=False, help_text="Use the base project table, instead of a prefix.")

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
            raise ValidationError(_('Date in future: %s' % from_date))
        return from_date

    def clean_to_date(self):
        to_date = self.cleaned_data.get('to_date', '')
        if to_date > date.today():
            raise ValidationError(_('Date in future: %s' % to_date))
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
        if to_date == None or from_date == None:
            return
        if to_date < from_date:
            raise forms.ValidationError("FromDate(%s) cannot be > ToDate(%s)" % (from_date, to_date))
        elif (to_date - from_date).days > 31:
            raise forms.ValidationError("FromDate(%s)-ToDate(%s) cannot be > 31 days" % (from_date, to_date))
        if not cleaned_data.get('table_prefix', None) and not cleaned_data.get('base_table', False):
            raise forms.ValidationError("Must select either 'Base Table' or give a table prefix")


class DBSyncFailReportForm(forms.Form):
    project = forms.ChoiceField(choices=[(x, x.capitalize()) for x in projects])
    from_date = forms.DateTimeField(initial=datetime.fromordinal((datetime.now() - timedelta(1)).toordinal()))
    to_date = forms.DateTimeField(initial=datetime.fromordinal((datetime.now()).toordinal()) - timedelta(seconds=1))
    delta_value = forms.IntegerField(initial=3600, label="Time Delta (secs)")

    def clean_from_date(self):
        from_date = self.cleaned_data.get('from_date', '')
        if from_date.replace(tzinfo=None) > datetime.now():
            raise ValidationError(_('Date/Time in future: %s' % from_date))
        return from_date

    def clean_to_date(self):
        to_date = self.cleaned_data.get('to_date', '')
        if to_date.replace(tzinfo=None) > datetime.now():
            raise ValidationError(_('Date/Time in future: %s' % to_date))
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
        cleaned_data = super(DBSyncFailReportForm, self).clean()
        from_date = cleaned_data.get("from_date")
        to_date = cleaned_data.get("to_date")
        if to_date == None or from_date == None:
            return
        if to_date < from_date:
            raise forms.ValidationError("FromDate(%s) cannot be > ToDate(%s)" % (from_date, to_date))
        elif (to_date - from_date).days > 31:
            raise forms.ValidationError("FromDate(%s)-ToDate(%s) cannot be > 31 days" % (from_date, to_date))


class DBReportForm(forms.Form):
    project = forms.ChoiceField(choices=[(x, x.capitalize()) for x in projects])
    from_date = forms.DateTimeField(initial=datetime.fromordinal((datetime.now() - timedelta(1)).toordinal()))
    to_date = forms.DateTimeField(initial=datetime.fromordinal((datetime.now()).toordinal()) - timedelta(seconds=1))

    def clean_from_date(self):
        from_date = self.cleaned_data.get('from_date', '')
        if from_date.replace(tzinfo=None) > datetime.now():
            raise ValidationError(_('Date/Time in future: %s' % from_date))
        return from_date

    def clean_to_date(self):
        to_date = self.cleaned_data.get('to_date', '')
        if to_date.replace(tzinfo=None) > datetime.now():
            raise ValidationError(_('Date/Time in future: %s' % to_date))
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
        if to_date == None or from_date == None:
            return
        if to_date < from_date:
            raise forms.ValidationError("FromDate(%s) cannot be > ToDate(%s)" % (from_date, to_date))
        elif (to_date - from_date).days > 31:
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
    reload_data = form.cleaned_data['reload_data']
    summary = {}
    summary["start"] = from_datetime
    summary["end"] = to_datetime
    event_classes = T3_EVENT_CLASS_FILE_PREFIXES[event_class]
    if table_prefix:
        table_name_prefix = "%s_%s" % (table_prefix, project)
    else:
        table_name_prefix = "%s" % project
    if isinstance(event_classes, list):
        summary["event_classes"] = event_classes
        for ev_class in event_classes:
            if "table_name" in summary:
                summary["table_name"] += ", "
            summary["table_name"] = table_name_prefix + "_nm_" + T3_EVENT_CLASS_FILE_PREFIXES[ev_class]
    else:
        summary["event_classes"] = event_class
        summary["table_name"] = table_name_prefix + "_nm_" + T3_EVENT_CLASS_FILE_PREFIXES[event_class]
    t3_redshift_config = get_t3_redshift_config(project, projects_cfg.get(project, 'report_config_file'))
    if table_prefix:
        create_tables(logger, project, t3_redshift_config, event_class, table_prefix)
    if reload_data:
        delete_logs(logger, project, t3_redshift_config, event_class, from_datetime, to_datetime, table_prefix)
    upload_stats = upload_logs(logger, project, t3_redshift_config, event_class, from_datetime, to_datetime,
                               table_prefix)
    report_data = {'summary': summary, 'upload_stats': upload_stats,
                   "general_config": t3_redshift_config["general_config"]}
    return render_to_response('upload_report.html', report_data,
                              context_instance=RequestContext(request))


def deletable_table_prefixes(project):
    logger = logging.getLogger('telemetry').getChild('DBDeleteLogsForm')
    t3_redshift_config = get_t3_redshift_config(project, projects_cfg.get(project, 'report_config_file'))
    public_tables = [x for x in list_tables(logger, t3_redshift_config) if x[0] == 'public' and "_nm_" in x[1]]
    choices_dict = {}
    for table in public_tables:
        prefix, rest = table[1].split('_nm_')
        if prefix not in projects and prefix.endswith(project):
            p = prefix.split("_" + project)[0]
            choices_dict[p] = p
    return choices_dict.keys()


def deleteLogsDefaultPage(request):
    message = ''
    form = DBDeleteLogsForm()
    proj = request.session.get('project', default_project)
    form.fields['project'].initial = proj
    return render_to_response('db_delete_logs.html', {'form': form, 'message': message,
                                                      'prefixes': deletable_table_prefixes(proj)},
                              context_instance=RequestContext(request))


@nachotoken_required
def db_delete_logs(request):
    logger = logging.getLogger('telemetry').getChild('db')
    if request.method != 'POST':
        return deleteLogsDefaultPage(request)

    form = DBDeleteLogsForm(request.POST)
    form.full_clean()

    project = form.cleaned_data['project']
    request.session['project'] = project

    if not form.is_valid():
        logger.warn('invalid form data')
        return deleteLogsDefaultPage(request)

    t3_redshift_config = get_t3_redshift_config(project, projects_cfg.get(project, 'report_config_file'))
    from_date = form.cleaned_data['from_date']
    to_date = form.cleaned_data['to_date']
    event_class = form.cleaned_data['event_class']
    table_prefix = form.cleaned_data['table_prefix']
    logger.debug("Deleting data from the database Project:%s, "
                 "From Date:%s, To Date:%s, Event Class=%s, Table Prefix:%s",
                 project, from_date, to_date, event_class, table_prefix)

    request.session['project'] = project
    request.session['event_class'] = event_class
    from_datetime = UtcDateTime(from_date) if from_date else None
    to_datetime = UtcDateTime(to_date) if to_date else None
    summary = {}
    summary["start"] = from_datetime
    summary["end"] = to_datetime
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
    delete_stats = delete_logs(logger, project, t3_redshift_config, event_class, from_datetime, to_datetime, table_prefix)
    report_data = {'summary': summary, 'delete_stats': delete_stats,
                   "general_config": t3_redshift_config["general_config"], 'message': ''}
    return render_to_response('delete_report.html', report_data,
                              context_instance=RequestContext(request))


class DBDeleteTableForm(forms.Form):
    project = forms.ChoiceField(choices=[(x, x.capitalize()) for x in projects])
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

    def clean_project(self):
        project = self.cleaned_data.get('project', '')
        if project not in projects:
            self.add_error('project', 'Unknown Project')
            raise ValidationError(_('Unknown Project: %(project)s'),
                                  code='unknown',
                                  params={'project': projects})
        return project


def deleteTablesDefaultPage(request):
    message = ''
    form = DBDeleteTableForm()
    proj = request.session.get('project', default_project)
    form.fields['project'].initial = proj
    return render_to_response('db_delete_tables.html', {'form': form, 'message': message,
                                                        'prefixes': deletable_table_prefixes(proj)},
                              context_instance=RequestContext(request))


@nachotoken_required
def db_delete_tables(request):
    logger = logging.getLogger('telemetry').getChild('db')
    if request.method != 'POST':
        return deleteTablesDefaultPage(request)

    form = DBDeleteTableForm(request.POST)
    form.full_clean()

    project = form.cleaned_data['project']
    request.session['project'] = project

    if not form.is_valid():
        logger.warn('invalid form data')
        return deleteTablesDefaultPage(request)

    t3_redshift_config = get_t3_redshift_config(project, projects_cfg.get(project, 'report_config_file'))
    table_prefix = form.cleaned_data['table_prefix']
    logger.debug("Deleting tables from the database Project:%s, Table Prefix:%s", project, table_prefix)

    request.session['project'] = project
    delete_tables(logger, project, t3_redshift_config, table_prefix)
    return deleteTablesDefaultPage(request)


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
    kwargs = {}
    kwargs['project'] = form.cleaned_data['project']
    kwargs['from_date'] = UtcDateTime(form.cleaned_data['from_date'])
    kwargs['to_date'] = UtcDateTime(form.cleaned_data['to_date'])
    return HttpResponseRedirect(reverse(db_log_report, kwargs=kwargs))


@nachotoken_required
def db_syncfail_report_form(request):
    logger = logging.getLogger('telemetry').getChild('db')
    # Any message set in 'message' will be displayed as a red error message.
    # Used for reporting error in any POST.
    message = ''
    if request.method != 'POST':
        form = DBSyncFailReportForm()
        form.fields['project'].initial = request.session.get('project', default_project)
        return render_to_response('syncfail_form.html', {'form': form, 'message': message},
                                  context_instance=RequestContext(request))

    form = DBSyncFailReportForm(request.POST)
    if not form.is_valid():
        logger.warn('invalid form data')
        return render_to_response('syncfail_form.html', {'form': form, 'message': message},
                                  context_instance=RequestContext(request))
    kwargs = {}
    kwargs['project'] = form.cleaned_data['project']
    kwargs['from_date'] = UtcDateTime(form.cleaned_data['from_date'])
    kwargs['to_date'] = UtcDateTime(form.cleaned_data['to_date'])
    kwargs['delta_value'] = form.cleaned_data['delta_value']
    return HttpResponseRedirect(reverse(db_syncfail_report, kwargs=kwargs))


def db_syncfail_report(request, project, from_date, to_date, delta_value):
    logger = logging.getLogger('telemetry').getChild('db')
    logger.debug("Running syncfail report for Project:%s, "
                 "From Date:%s, To Date:%s, Time Delta Value:%d",
                 project, from_date, to_date, int(delta_value))
    request.session['project'] = project
    from_datetime = UtcDateTime(from_date)
    to_datetime = UtcDateTime(to_date)
    summary = {}
    summary["start"] = from_datetime
    summary["end"] = to_datetime

    t3_redshift_config = get_t3_redshift_config(project, projects_cfg.get(project, 'report_config_file'))
    error_list = []
    warning_list = []
    summary, device_list = syncfail_report(logger, t3_redshift_config['general_config']['project'],
                                           t3_redshift_config, from_datetime, to_datetime, int(delta_value))
    report_data = {'summary': summary, 'devices': device_list, 'general_config': t3_redshift_config["general_config"]}
    return render_to_response('syncfail_report.html', report_data,
                              context_instance=RequestContext(request))


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
    summary["end"] = to_datetime
    t3_redshift_config = get_t3_redshift_config(project, projects_cfg.get(project, 'report_config_file'))
    error_list = []
    warning_list = []
    summary, error_list, warning_list = log_report(logger, t3_redshift_config['general_config']['project'],
                                                   t3_redshift_config, from_datetime, to_datetime)
    clustered_error_list = classify_log_events(error_list)
    clustered_warning_list = classify_log_events(warning_list)
    report_data = {'summary': summary, 'clustered_errors': clustered_error_list,
                   'clustered_warnings': clustered_warning_list,
                   'errors': error_list, 'warnings': warning_list,
                   "general_config": t3_redshift_config["general_config"]}
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
    error, col_names, results = execute_sql(logger, project, t3_redshift_config, sql_query, entry_page)
    report_data = {'results': results, 'rowcount': len(results), 'col_names': col_names, 'message': error,
                   "general_config": t3_redshift_config["general_config"], 'form': form}
    return render_to_response('db_query.html', report_data,
                              context_instance=RequestContext(request))
