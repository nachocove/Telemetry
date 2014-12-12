import base64
from functools import wraps
from gettext import gettext as _
import hashlib
import os
import sys
import dateutil.parser
from datetime import timedelta, datetime
from django.conf import settings
from django.core.exceptions import ValidationError
from django.core.urlresolvers import reverse
from django.http import HttpResponseBadRequest
from django import forms
from django.shortcuts import render, render_to_response
from django.http import HttpResponseRedirect
import logging
import cgi
import json
import ConfigParser
from django.template import RequestContext
from django.utils.decorators import available_attrs

sys.path.append('../scripts')

from PyWBXMLDecoder.ASCommandResponse import ASCommandResponse
from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.dynamodb2.exceptions import DynamoDBError
from AWS.query import Query
from AWS.selectors import SelectorEqual, SelectorLessThanEqual
from AWS.tables import TelemetryTable
from monitors.monitor_base import Monitor
from misc.support import Support
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


class LoginForm(forms.Form):
    #username = forms.CharField()
    password = forms.CharField(widget=forms.PasswordInput)

    def clean_password(self):
        password = self.cleaned_data.get('password', '')
        if hashlib.sha256(password).hexdigest() != settings.NACHO_PASSWORD_DIGEST:
            self.add_error('password', 'Incorrect Password')
        return password

class VectorForm(forms.Form):
    project = forms.ChoiceField(choices=[(x, x.capitalize()) for x in projects])
    tele_paste = forms.CharField(widget=forms.Textarea)

    def clean_project(self):
        project = self.cleaned_data.get('project', '')
        if project not in projects:
            self.add_error('project', 'Unknown Project')
            raise ValidationError(_('Unknown Project: %(project)s'),
                                  code='unknown',
                                  params={'project': projects})
        return project


_aws_connection_cache = {}
def _aws_connection(project):
    global _aws_connection_cache
    if not project in _aws_connection_cache:
        if not project in projects:
            raise ValueError('Project %s is not present in projects.cfg' % project)
        _aws_connection_cache[project] = DynamoDBConnection(host='dynamodb.us-west-2.amazonaws.com',
                                                            port=443,
                                                            aws_secret_access_key=projects_cfg.get(project, 'secret_access_key'),
                                                            aws_access_key_id=projects_cfg.get(project, 'access_key_id'),
                                                            region='us-west-2',
                                                            is_secure=True)
    TelemetryTable.PREFIX = project
    return _aws_connection_cache[project]


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
            retval[mapping[key]] = value.strip()
    logger = logging.getLogger('telemetry').getChild('_parse_junk')
    logger.debug('retval=%s', retval)
    return retval


def _parse_crash_report(junk):
    dict_ = _parse_junk(junk, {'Device ID': 'device_id', 'Date/Time': 'timestamp'})
    if 'device_id' in dict_ and 'timestamp' in dict_:
        return dict_
    return None


def _parse_error_report(junk):
    dict_ = _parse_junk(junk, {'timestamp': 'timestamp', 'client': 'client'})
    if 'timestamp' in dict_ and 'client' in dict_:
        return dict_
    return None


def _parse_support_email(junk):
    dict_ = _parse_junk(junk, {'email': 'email', 'timestamp': 'timestamp'})
    if 'email' in dict_:
        return dict_
    return None

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
        if validate_session(request):
            return view_func(request, *args, **kwargs)
        else:
            return HttpResponseRedirect(settings.LOGIN_URL)
    return _wrapped_view

# Create your views here.
def login(request):
    message = ''
    logger = logging.getLogger('telemetry').getChild('login')
    if request.method == 'POST':
        form = LoginForm(request.POST)
        if form.is_valid():
            try:
                create_session(request)
                return HttpResponseRedirect(request.GET.get('next', '/'))
            except Exception, e:
                logger.error('fail to get session token - %s' % str(e))
                message = 'Cannot log in (%s). Please enter the password again.' % e
    else:
        form = LoginForm()
    return render(request, 'login.html', {'form': form, 'message': message})

def logout(request):
    try:
        del request.session['nachotoken']
    except AttributeError:
        pass
    return HttpResponseRedirect(settings.LOGIN_URL)

def process_error_report(request, project, form, loc, logger):
    loc['span'] = str(default_span)
    return HttpResponseRedirect(reverse(entry_page, kwargs={'client': loc['client'],
                                                            'timestamp': loc['timestamp'],
                                                            'span': loc['span'],
                                                            'project': project}))

def process_email(request, project, form, loc, logger):
    loc['span'] = str(default_span)
    # From the email, we need to find the client ID
    conn = _aws_connection(project)
    query = Query()
    query.add('event_type', SelectorEqual('SUPPORT'))
    events = Query.events(query, conn)
    email_events = Support.get_sha256_email_address(events, loc['email'])[1]
    if len(email_events) != 0:
        clients = {}
        # loop over the sorted email events, oldest first. The result is a dict of client-id's
        # where the value is a dict containing the first time we've seen this client-id and
        # the last time we saw this client-id.
        for ev in sorted(email_events, reverse=False, key=lambda x: x.timestamp):
            if ev.client not in clients:
                clients[ev.client] = {'first': ev.timestamp,
                                      'timestamp': ev.timestamp,
                                      'client': ev.client,
                                      'span': str(default_span),
                                      }
            clients[ev.client]['timestamp'] = ev.timestamp
        # fill in the URL's for each client item.
        for k in clients:
            clients[k]['url'] = reverse(entry_page, kwargs={'client': clients[k]['client'],
                                                            'timestamp': clients[k]['timestamp'],
                                                            'span': clients[k]['span'],
                                                            'project': project})
        # make it into a list
        clients = sorted(clients.values(), key=lambda x: x['timestamp'], reverse=True)
        # if we found only one, just redirect.
        if len(clients) == 1:
            client = clients[0]
            logger.debug('client=%(client)s, span=%(span)s', client)
            return HttpResponseRedirect(client['url'])
        else:
            return render_to_response('client_picker.html', {'clients': clients, 'project': project},
                                      context_instance=RequestContext(request))
    else:
        message = 'Cannot find client ID for email %s' % loc['email']
        logger.warn(message)
    return render_to_response('home.html', {'form': form, 'message': message},
                              context_instance=RequestContext(request))

def process_crash_report(request, project, form, loc, logger):
    loc['span'] = str(default_span)

    # From the crash log device ID, we need to find the client ID
    conn = _aws_connection(project)

    # first, see if the device-id can be found in the device_info table
    query = Query()
    query.add('device_id', SelectorEqual(loc['device_id']))
    if 'timestamp' in loc:
        utc_timestamp = UtcDateTime(loc['timestamp'])
        query.add('uploaded_at', SelectorLessThanEqual(utc_timestamp))
    devices = Query.users(query, conn)
    if not devices:
        message = 'Cannot find client ID for device ID %s' % loc['device_id']
        logger.warn(message)
        return render_to_response('home.html', {'form': form, 'message': message},
                                  context_instance=RequestContext(request))

    clients = {}

    # Violating DRY here (see process_email()). Should refactor this when I have time.
    for device in sorted(devices, reverse=False, key=lambda x: x['timestamp']):
        if device['client'] not in clients:
            clients[device['client']] = {'first': device['timestamp'],
                                         'timestamp': device['timestamp'],
                                         'client': device['client'],
                                         'span': str(default_span),
                                         }
        clients[device['client']]['timestamp'] = device['timestamp']
    # fill in the URL's for each client item.
    for k in clients:
        clients[k]['url'] = reverse(entry_page, kwargs={'client': clients[k]['client'],
                                                        'timestamp': clients[k]['timestamp'],
                                                        'span': clients[k]['span'],
                                                        'project': project})
    # make it into a list
    clients = sorted(clients.values(), key=lambda x: x['timestamp'], reverse=True)
    # if we found only one, just redirect.
    if len(clients) == 1:
        client = clients[0]
        logger.debug('client=%(client)s, span=%(span)s', client)
        return HttpResponseRedirect(client['url'])
    else:
        return render_to_response('client_picker.html', {'clients': clients, 'project': project},
                                  context_instance=RequestContext(request))

@nachotoken_required
def home(request):
    logger = logging.getLogger('telemetry').getChild('home')
    # Any message set in 'message' will be displayed as a red error message.
    # Used for reporting error in any POST.
    message = ''
    if request.method != 'POST':
        form = VectorForm()
        form.fields['project'].initial = request.session.get('project', default_project)
        return render_to_response('home.html', {'form': form, 'message': message},
                                  context_instance=RequestContext(request))

    form = VectorForm(request.POST)
    if not form.is_valid():
        logger.warn('invalid form data')
        return render_to_response('home.html', {'form': form, 'message': message},
                                  context_instance=RequestContext(request))

    logger.debug('tele_paste=%s', form.cleaned_data['tele_paste'])
    loc = _parse_error_report(form.cleaned_data['tele_paste'])
    project = form.cleaned_data['project']
    request.session['project'] = project
    paste_data = form.cleaned_data['tele_paste']
    if loc is not None:
        return process_error_report(request, project, form, loc, logger)

    loc = _parse_support_email(paste_data)
    if loc is not None:
        return process_email(request, project, form, loc, logger)

    loc = _parse_crash_report(paste_data)
    if loc is not None:
        return process_crash_report(request, project, form, loc, logger)

    # if we got here, we couldn't figure out what to process
    logger.warn('Unable to parse pasted info.')
    message = 'Unable to parse pasted info. Please Try again.'
    return render_to_response('home.html', {'form': form, 'message': message},
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
        raise TypeError, 'Object of type %s with value of %s is not JSON serializable' % (type(obj), repr(obj))

@nachotoken_required
def entry_page_legacy(request, client='', timestamp='', span=str(default_span)):
    project = os.environ.get('PROJECT', '')
    if not project:
        project = projects[0]
    return entry_page(request, project, client=client, timestamp=timestamp, span=span)

@nachotoken_required
def entry_page(request, project='', client='', timestamp='', span=str(default_span)):
    logger = logging.getLogger('telemetry').getChild('entry_page')
    logger.info('client=%s, timestamp=%s, span=%s', client, timestamp, span)
    span = int(span)
    client = str(client)
    center = dateutil.parser.parse(timestamp)
    spread = timedelta(minutes=int(span))
    after = center - spread
    before = center + spread
    go_earlier = after - spread
    go_later = before + spread

    context = entry_page_base(project, client, after, before, logger)

    iso_center = _iso_z_format(center)
    iso_go_earlier = _iso_z_format(go_earlier)
    iso_go_later = _iso_z_format(go_later)
    # Add buttons
    def ctrl_url(client_, time_, span_):
        return reverse(entry_page, kwargs={'client': client_,
                                           'timestamp': time_,
                                           'span': span_,
                                           'project': project})
    context['buttons'] = []
    zoom_in_span = max(1, span/2)
    context['buttons'].append({'text': 'Zoom in (%d min)' % zoom_in_span,
                               'url': ctrl_url(client, iso_center, zoom_in_span),
                               })
    context['buttons'].append({'text': 'Zoom out (%d min)' % (span*2),
                               'url': ctrl_url(client, iso_center, span*2),
                               })
    context['buttons'].append({'text': 'Go back %d min' % (2*span),
                               'url': ctrl_url(client, iso_go_earlier, span),
                               })
    context['buttons'].append({'text': 'Go forward %d min' % (2*span),
                               'url': ctrl_url(client, iso_go_later, span),
                               })
    context['body_args'] = 'onload=refresh()'
    return render_to_response('entry_page.html', context,
                              context_instance=RequestContext(request))

def entry_page_by_timestamps(request, project, client='', after='', before=''):
    logger = logging.getLogger('telemetry').getChild('entry_page')
    logger.info('client=%s, after=%s, before=%s', client, after, before)
    context = entry_page_base(project, client, after, before, logger)
    context['body_args'] = 'onload=refresh()'
    return render_to_response('entry_page.html', context,
                              context_instance=RequestContext(request))

def entry_page_base(project, client, after, before, logger):
    conn = _aws_connection(project)
    query = Query()
    query.limit = 100000
    query.add('client', SelectorEqual(client))
    query.add_range('timestamp', UtcDateTime(str(after)), UtcDateTime(str(before)))
    ###### FIXME - logger.debug('query=%s', str(query.where()))
    obj_list = list()
    event_count = 0
    logger.info('project = %s', project)
    try:
        (obj_list, event_count) = Monitor.query_events(conn, query, False, logger)
        logger.info('%d objects found', len(obj_list))
    except DynamoDBError, e:
        logger.error('fail to query events - %s', str(e))

    # Save some global parameters for summary table
    params = dict()
    params['start'] = after
    params['stop'] = before
    params['client'] = client
    params['event_count'] = event_count
    # Query the user device info
    try:
        user_query = Query()
        user_query.add('client', SelectorEqual(client))
        client_list = Query.users(user_query, conn)

        if len(client_list) > 0:
            # Get the user from the first client
            params['os_type'] = client_list[0]['os_type']
            params['os_version'] = client_list[0]['os_version']
            params['device_model'] = client_list[0]['device_model']
            params['build_version'] = client_list[0]['build_version']
    except DynamoDBError, e:
        return HttpResponseBadRequest('fail to query device info - %s', str(e))

    context = {'project': project,
               'params': json.dumps(params, default=json_formatter),
               }

    # Generate the events JSON
    event_list = [dict(x.items()) for x in obj_list]
    show_hide_list = set()
    for event in event_list:
        event['timestamp'] = str(event['timestamp'])
        for field in ['uploaded_at', 'client']:
            del event[field]

        if event['event_type'] in ['WBXML_REQUEST', 'WBXML_RESPONSE']:
            show_hide_list.add('wbxml')
            def decode_wbxml(wbxml_):
                instance = ASCommandResponse(base64.b64decode(wbxml_))
                return instance.xmlString

            b64 = event['wbxml'].encode()
            event['wbxml_base64'] = cgi.escape(b64)
            event['wbxml'] = cgi.escape(decode_wbxml(b64))
        else:
            show_hide_list.add(event['event_type'].lower())

        if 'message' in event:
            event['message'] = cgi.escape(event['message'])

    context['events'] = json.dumps(event_list, default=json_formatter)
    context['dropdown'] = show_hide_list
    return context
