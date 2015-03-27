# Copyright 2014, NachoCove, Inc
import logging
from django.core.urlresolvers import reverse
from django import forms
from django.http import HttpResponseRedirect
from django.shortcuts import render_to_response
from django.template import RequestContext
from django.views.decorators.csrf import csrf_exempt

from AWS.query import Query
from core.connection import aws_connection, projects, default_project
from core.dates import iso_z_format
from misc.utc_datetime import UtcDateTime

tmp_logger = logging.getLogger('telemetry')

custom_datetime_formats = ('%Y-%m-%dT%H:%M:%SZ',)
#from django.utils import formats
#datetime_input_formats = formats.get_format_lazy('DATETIME_INPUT_FORMATS') + custom_datetime_formats

class NachoDateTimeField(forms.DateTimeField):
    input_formats = custom_datetime_formats
    widget = forms.TextInput(attrs={'class': 'nachodatetimepicker',
                                    })

class ReportsForm(forms.Form):
    reports = {'emails_per_timeframe': {'description': 'Email Addresses Per domain'},
               }

    project = forms.ChoiceField(choices=[(x, x.capitalize()) for x in projects])
    report = forms.ChoiceField(choices=[(x, reports[x]['description']) for x in reports])
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


    if form.cleaned_data['report'] == 'emails_per_timeframe':
        return HttpResponseRedirect(reverse(emails_per_timeframe,
                                            kwargs={'end': iso_z_format(form.cleaned_data['end']),
                                                    'start': iso_z_format(form.cleaned_data['start']),
                                                    'project': form.cleaned_data['project']}))
    else:
        return render_to_response('reports_home.html', {'form': form},
                                  context_instance=RequestContext(request))


def emails_per_domain(email_addresses):
    emails_per_domain_dict = dict()
    for email in email_addresses:
        userhash, domain = email.split('@')
        if domain not in emails_per_domain_dict:
            emails_per_domain_dict[domain] = 0
        emails_per_domain_dict[domain] += 1
    return emails_per_domain_dict

def emails_per_timeframe(request, project, start, end):
    conn = aws_connection(project)
    logger = tmp_logger.getChild('emails_per_timeframe')
    email_addresses = Query.emails_per_domain(UtcDateTime(start), UtcDateTime(end), conn, logger=logger)
    per_domain = emails_per_domain(email_addresses)
    return render_to_response('email_report.html', {'emails': email_addresses,
                                                    'emails_per_domain': [{'name': x, 'count': per_domain[x]} for x in per_domain],
                                                    'start': start,
                                                    'end': end,
                                                    'number_of_domains': len(per_domain.keys()),
                                                    },
                                      context_instance=RequestContext(request))


