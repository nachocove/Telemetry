# Copyright 2014, NachoCove, Inc
from datetime import datetime
import logging
from django.core.urlresolvers import reverse
from django import forms
from django.http import HttpResponseRedirect
from django.shortcuts import render_to_response
from django.template import RequestContext
from django.views.decorators.csrf import csrf_exempt

from core.connection import aws_connection, projects, default_project, aws_s3_connection, projects_cfg
from core.dates import iso_z_format
from misc.utc_datetime import UtcDateTime
from monitors.monitor_base import Summary
from monitor import report_mapping

tmp_logger = logging.getLogger('telemetry')

custom_datetime_formats = ('%Y-%m-%dT%H:%M:%SZ',)
#from django.utils import formats
#datetime_input_formats = formats.get_format_lazy('DATETIME_INPUT_FORMATS') + custom_datetime_formats

class NachoDateTimeField(forms.DateTimeField):
    input_formats = custom_datetime_formats
    widget = forms.TextInput(attrs={'class': 'nachodatetimepicker',
                                    })

    def to_python(self, value):
        if isinstance(value, (str, unicode)) and value == 'now':
            return datetime.now()
        return super(NachoDateTimeField, self).to_python(value)

    def validate(self, value):
        return UtcDateTime(value)

class ReportsForm(forms.Form):
    project = forms.ChoiceField(choices=[(x, x.capitalize()) for x in projects], )
    report = forms.ChoiceField(choices=[(x, report_mapping[x]['description'])
                                        for x in sorted(report_mapping) if report_mapping[x]["description"]],
                               initial=sorted([k
                                               for k in report_mapping.keys() if report_mapping[k]["description"]])[0])
    start = NachoDateTimeField()
    end = NachoDateTimeField()

@csrf_exempt
def home(request):
    """

    :param request:  django.http.HttpRequest
    :return:
    """
    logger = tmp_logger.getChild('home')
    if request.method == "GET":
        form = ReportsForm()
        form.fields['project'].initial = request.session.get('project', default_project)
        return render_to_response('reports_home.html', {'form': form},
                                  context_instance=RequestContext(request))


    form = ReportsForm(request.POST)
    if not form.is_valid():
        logger.error("Form data not correct")
        return render_to_response('reports_home.html', {'form': form},
                                  context_instance=RequestContext(request))


    if form.cleaned_data['report']:
        return HttpResponseRedirect(reverse(monitor_reports,
                                            kwargs={'report': form.cleaned_data['report'],
                                                    'end': iso_z_format(form.cleaned_data['end']),
                                                    'start': iso_z_format(form.cleaned_data['start']),
                                                    'project': form.cleaned_data['project']}))
    else:
        return render_to_response('reports_home.html', {'form': form},
                                  context_instance=RequestContext(request))


def monitor_reports(request, report, project, start, end):
    conn = aws_connection(project)
    summary_table = Summary()

    if report not in report_mapping:
        raise Exception('unknown report')

    kwargs = {'start': UtcDateTime(start),
              'end': UtcDateTime(end),
              'prefix': project,
              'attachment_dir': None,
              }

    if report.startswith('pinger'):
        s3conn = aws_s3_connection(project)
        kwargs.update({
              's3conn': s3conn,
              'bucket_name': projects_cfg.get(project, 'telemetry_bucket'),
              'path_prefix': projects_cfg.get(project, 'telemetry_prefix'),
        })

    monitor = report_mapping[report]["klass"](conn=conn, **kwargs)
    monitor.run()
    results = monitor.report(summary_table)
    return render_to_response('monitor_reports.html', {'title': monitor.title(),
                                                       'summary': summary_table.html(),
                                                       'search_results': results.html() if results else "",
                                                       'project': project,
                                                       'start': start,
                                                       'end': end,
                                                       },
                              context_instance=RequestContext(request))
