import base64
from functools import wraps
from gettext import gettext as _
import hashlib
import os
from datetime import timedelta, datetime
import logging
import cgi
import json
import ConfigParser
import re
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
from boto.dynamodb2.exceptions import DynamoDBError
import zlib

from misc import events
from PyWBXMLDecoder.ASCommandResponse import ASCommandResponse
from AWS.query import Query
from AWS.selectors import SelectorEqual, SelectorLessThanEqual, SelectorBetween, SelectorContains, SelectorGreaterThanEqual
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

BOTO_DEBUG=False
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
                                                            is_secure=True,
                                                            debug=2 if BOTO_DEBUG else 0)
    TelemetryTable.PREFIX = project
    return _aws_connection_cache[project]

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
    TelemetryTable.PREFIX = project
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


def _parse_crash_report(junk):
    dict_ = _parse_junk(junk, {'Device ID': 'device_id',
                               'Date/Time': 'timestamp',
                               'Launch Time': 'timestamp'})
    if 'device_id' in dict_ and 'timestamp' in dict_:
        return dict_
    return None


def _parse_error_report(junk):
    dict_ = _parse_junk(junk, {'timestamp': 'timestamp', 'client': 'client'})
    if 'timestamp' in dict_ and 'client' in dict_:
        return dict_
    return None


def _parse_support_email(junk):
    dict_ = _parse_junk(junk, {'email': 'email', 'timestamp': 'timestamp', 'span': 'span'})
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

def client_ids_from_email(email, after, before, project, include_url=True):
    conn = _aws_connection(project)
    query = Query()
    query.add('event_type', SelectorEqual('SUPPORT'))
    if after and before:
        query.add_range('uploaded_at', after, before)
    events = Query.events(query, conn)
    if not events:
        return {}

    (_, email_events) = Support.get_email_address_clients(events, email)
    clients = {}
    if len(email_events) > 0:
        # loop over the sorted email events, oldest first. The result is a dict of client-id's
        # where the value is a dict containing the first time we've seen this client-id and
        # the last time we saw this client-id.
        for ev in sorted(email_events, reverse=False, key=lambda x: x.timestamp):
            if ev.client not in clients:
                clients[ev.client] = {'first': ev.timestamp,
                                      'timestamp': ev.timestamp,
                                      'client': ev.client,
                                      'span': str(default_span),
                                      'email': ev.sha256_email_address,
                                      }
            clients[ev.client]['timestamp'] = ev.timestamp
        if include_url:
            # fill in the URL's for each client item.
            for k in clients:
                clients[k]['url'] = reverse(entry_page, kwargs={'client': clients[k]['client'],
                                                                'timestamp': clients[k]['timestamp'],
                                                                'span': clients[k]['span'],
                                                                'project': project})
    return clients

def process_email(request, project, form, loc, logger):
    if loc.get('timestamp', None):
        after, before = calc_spread_from_center(loc['timestamp'], span=loc.get('span', 16))
    else:
        after = None
        before = None

    clients = client_ids_from_email(loc['email'], after, before, project)
    if clients:
        # make it into a list
        clients = sorted(clients.values(), key=lambda x: x['timestamp'], reverse=True)
    else:
        clients = []

    loc['span'] = str(default_span)
    # if we found only one, just redirect.
    if len(clients) == 1:
        client = clients[0]
        logger.debug('client=%(client)s, span=%(span)s', client)
        return HttpResponseRedirect(client['url'])
    elif len(clients) > 1:
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
    project = form.cleaned_data['project']
    request.session['project'] = project
    paste_data = form.cleaned_data['tele_paste']

    loc = _parse_error_report(paste_data)
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
        try:
            return str(obj)
        except Exception as e:
            raise TypeError, 'Object of type %s with value of %s not converted to string: %s' % (type(obj), repr(obj), e)

@nachotoken_required
@nacho_cache
def entry_page_legacy(request, client='', timestamp='', span=str(default_span)):
    project = os.environ.get('PROJECT', '')
    if not project:
        project = projects[0]
    return entry_page(request, project, client=client, timestamp=timestamp, span=span)

def ctrl_url(client, time, span, project):
    return reverse(entry_page, kwargs={'client': client,
                                       'timestamp': time,
                                       'span': span,
                                       'project': project})

def calc_spread_from_center(center, span=default_span):
    if not isinstance(center, datetime):
        center = dateutil.parser.parse(center)
    spread = timedelta(minutes=int(span))
    return UtcDateTime(center-spread), UtcDateTime(center+spread)

def calc_spread(after, before, span=default_span, center=None):
    if not isinstance(after, datetime):
        after = dateutil.parser.parse(after)
    if not isinstance(before, datetime):
        before = dateutil.parser.parse(before)
    span = int(span)
    if center is None:
        diff = (before - after)/2
        center = after + diff
    spread = timedelta(minutes=int(span))
    go_earlier = after - spread
    go_later = before + spread
    iso_center = _iso_z_format(center)
    iso_go_earlier = _iso_z_format(go_earlier)
    iso_go_later = _iso_z_format(go_later)
    return iso_go_earlier, iso_center, iso_go_later

@nachotoken_required
@nacho_cache
def entry_page(request, project='', client='', timestamp='', span=str(default_span)):
    logger = logging.getLogger('telemetry').getChild('entry_page')
    logger.info('client=%s, timestamp=%s, span=%s', client, timestamp, span)
    span = int(span)
    client = str(client)
    center = dateutil.parser.parse(timestamp)
    spread = timedelta(minutes=int(span))
    after = center - spread
    before = center + spread

    context = entry_page_base(project, client, after, before, request.GET, logger)

    iso_go_earlier, iso_center, iso_go_later = calc_spread(after, before, span=span, center=center)

    # Add buttons
    context['buttons'] = []
    zoom_in_span = max(1, span/2)
    context['buttons'].append({'text': 'Zoom in (%d min)' % zoom_in_span,
                               'url': ctrl_url(client, iso_center, zoom_in_span, project),
                               })
    context['buttons'].append({'text': 'Zoom out (%d min)' % (span*2),
                               'url': ctrl_url(client, iso_center, span*2, project),
                               })
    context['buttons'].append({'text': 'Go back %d min' % span,
                               'url': ctrl_url(client, iso_go_earlier, span, project),
                               })
    context['buttons'].append({'text': 'Go forward %d min' % span,
                               'url': ctrl_url(client, iso_go_later, span, project),
                               })
    context['body_args'] = 'onload=refresh()'
    return render_to_response('entry_page.html', context,
                              context_instance=RequestContext(request))

@nachotoken_required
@nacho_cache
def entry_page_by_timestamps(request, project, client='', after='', before=''):
    logger = logging.getLogger('telemetry').getChild('entry_page')
    logger.info('client=%s, after=%s, before=%s', client, after, before)
    context = entry_page_base(project, client, after, before, request.GET, logger)
    iso_go_earlier, iso_center, iso_go_later = calc_spread(after, before, span=default_span, center=None)
    context['buttons'] = []
    zoom_in_span = max(1, default_span/2)
    context['buttons'].append({'text': 'Zoom in (%d min)' % zoom_in_span,
                               'url': ctrl_url(client, iso_center, zoom_in_span, project),
                               })
    context['buttons'].append({'text': 'Zoom out (%d min)' % (default_span*2),
                               'url': ctrl_url(client, iso_center, default_span*2, project),
                               })
    context['buttons'].append({'text': 'Go back %d min' % default_span,
                               'url': ctrl_url(client, iso_go_earlier, default_span, project),
                               })
    context['buttons'].append({'text': 'Go forward %d min' % default_span,
                               'url': ctrl_url(client, iso_go_later, default_span, project),
                               })
    context['body_args'] = 'onload=refresh()'
    return render_to_response('entry_page.html', context,
                              context_instance=RequestContext(request))

def get_pinger_telemetry(project, client, after, before):
    conn = _aws_s3_connection(project)
    bucket_name = projects_cfg.get(project, 'telemetry_bucket')
    bucket = conn.get_bucket(bucket_name)
    # make sure to have the slash at the end, but not at the start
    prefix = os.path.join(projects_cfg.get(project, 'telemetry_prefix'), '').lstrip('/')
    file_regex = re.compile(r'%s%s--(?P<start>[0-9\-:TZ\.]+)--(?P<end>[0-9\-:TZ\.]+)\.json(?P<ext>.*)' % (prefix, 'log'))
    events = []
    for key in bucket.list(prefix):
        m = file_regex.match(key.key)
        if m is not None:
            start = UtcDateTime(m.group('start'))
            end = UtcDateTime(m.group('end'))
            if (start.datetime >= after.datetime and start.datetime < before.datetime) or \
                    (end.datetime >= after.datetime and end.datetime < before.datetime):
                if m.group('ext') == '.gz':
                    #http://stackoverflow.com/questions/1543652/python-gzip-is-there-a-way-to-decompress-from-a-string/18319515#18319515
                    json_str = zlib.decompress(key.get_contents_as_string(), 16+zlib.MAX_WBITS)
                elif m.group('ext') == '':
                    json_str = key.get_contents_as_string()
                else:
                    raise Exception("unknown extension %s" % m.group('ext'))
                for ev in json.loads(json_str):
                    if client and client != ev.get('client', ''):
                        continue

                    timestamp = UtcDateTime(ev['timestamp'])
                    if not (timestamp.datetime >= after.datetime and timestamp.datetime < before.datetime):
                        continue
                    ev['thread_id'] = ""
                    ev['timestamp'] = timestamp
                    ev['uploaded_at'] = UtcDateTime(ev['uploaded_at'])
                    events.append(ev)
    return events


def entry_page_base(project, client, after, before, params, logger):
    conn = _aws_connection(project)
    query = Query()
    query.limit = 100000
    query.add('client', SelectorEqual(client))
    query.add_range('timestamp', UtcDateTime(str(after)), UtcDateTime(str(before)))
    if params:
        if params.get('search', ''):
            field,search = params['search'].split(':')
            query.add(field, SelectorContains(search))
    ###### FIXME - logger.debug('query=%s', str(query.where()))
    event_count = 0
    logger.info('project = %s', project)
    event_list = []
    try:
        (obj_list, event_count) = Monitor.query_events(conn, query, False, logger)
        logger.info('%d objects found', len(obj_list))
        for obj in obj_list:
            ev = dict(obj.items())
            if not 'module' in ev:
                ev['module'] = ""
            event_list.append(ev)
    except DynamoDBError, e:
        logger.error('failed to query events - %s', str(e))

    pinger_objs = get_pinger_telemetry(project, client, UtcDateTime(str(after)), UtcDateTime(str(before)))
    if pinger_objs:
        event_list.extend(pinger_objs)
        event_count += len(pinger_objs)

    # Save some global parameters for summary table
    params = dict()
    params['start'] = after
    params['stop'] = before
    params['event_count'] = event_count
    # Query the user device info
    try:
        user_query = Query()
        user_query.add('client', SelectorEqual(client))
        user_query.add('uploaded_at', SelectorBetween(UtcDateTime(after), UtcDateTime(before)))
        client_list = Query.users(user_query, conn)
        if len(client_list) == 0:
            # widen the search :-(
            user_query = Query()
            user_query.add('client', SelectorEqual(client))
            client_list = Query.users(user_query, conn)

        if len(client_list) > 0:
            # Get the data from the LAST client-entry
            params.update(client_list[-1])
    except DynamoDBError, e:
        return HttpResponseBadRequest('fail to query device info - %s', str(e))

    params.setdefault('client', client)
    params.setdefault('device_id', '')
    params.setdefault('os_type', '')
    params.setdefault('os_version', '')
    params.setdefault('device_model', '')
    params.setdefault('build_version', '')
    params.setdefault('build_number', '')

    event_list = sorted(event_list, key=lambda x: x['timestamp'])
    # Generate the events JSON
    for event in event_list:
        for field in ['uploaded_at', 'client']:
            if field in event:
                del event[field]

        if event['event_type'] in ['WBXML_REQUEST', 'WBXML_RESPONSE']:
            def decode_wbxml(wbxml_):
                instance = ASCommandResponse(base64.b64decode(wbxml_))
                return '\n'.join(instance.xmlString.split('\n')[1:])

            b64 = event['wbxml'].encode()
            event['wbxml_base64'] = cgi.escape(b64)
            event['wbxml'] = cgi.escape(decode_wbxml(b64))
        if 'message' in event:
            event['message'] = cgi.escape(event['message'])
        if 'timestamp' in event:
            event['timestamp'] = str(event['timestamp'])

    context = {'project': project,
               'params': json.dumps(params, default=json_formatter),
               'events': json.dumps(event_list, default=json_formatter),
               }

    return context

def event_choices():
    ec = {}
    for ev in events.TYPES:
        if ev in ('WBXML_REQUEST', 'WBXML_RESPONSE'):
            continue  # omit these
        elif ev in ('INFO', 'DEBUG'):
            ec[ev] = ev.lower().capitalize() + " (DANGER: Lots of records!)"
        elif ev in ('UI',):
            continue
        else:
            ec[ev] = ev.lower().capitalize()
    return sorted([ (k, ec[k]) for k in ec ])

class SearchForm(forms.Form):
    EVENT_CHOICES = event_choices()
    project = forms.ChoiceField(choices=[(x, x.capitalize()) for x in projects])
    message = forms.CharField(help_text="(optional) Enter a substring to look for in the telemetry.log-message field", required=False)
    after = forms.CharField(help_text="(required) UTC timestamp in Z-format (e.g. 2015-01-30T19:34:25T)")
    before = forms.CharField(help_text="(required) UTC timestamp in Z-format (e.g. 2015-01-30T19:34:25T)")
    email = forms.CharField(help_text="(optional) Email of user (maybe be obfuscated already)", required=False)

    event_type = forms.MultipleChoiceField(choices=EVENT_CHOICES, widget=forms.CheckboxSelectMultiple(),
                                           help_text="Select the event-type to search in. Each one is a separate query!")

    def clean_after(self):
        after = self.cleaned_data.get('after', '')
        try:
            if after:
                return UtcDateTime(after)
            else:
                raise Exception("No after time given")
        except Exception as e:
            self.add_error('after', str(e))
            raise ValidationError(_('Bad After: %(after)s'),
                                  code='unknown',
                                  params={'after': after})

    def clean_before(self):
        before = self.cleaned_data.get('before', '')
        try:
            if before:
                return UtcDateTime(before)
            else:
                raise Exception("No before time given")
        except Exception as e:
            self.add_error('before', str(e))
            raise ValidationError(_('Bad before: %(before)s'),
                                  code='unknown',
                                  params={'before': before})


def search(request):
    logger = logging.getLogger('telemetry').getChild('search')
    # Any message set in 'message' will be displayed as a red error message.
    # Used for reporting error in any POST.
    message = ''
    if request.method != 'POST':
        form = SearchForm()
        form.fields['project'].initial = request.session.get('project', default_project)
        form.fields['event_type'].initial = ('ERROR', 'WARN')
        return render_to_response('search.html', {'form': form, 'message': message},
                                  context_instance=RequestContext(request))
    form = SearchForm(request.POST)
    if not form.is_valid():
        logger.warn('invalid form data')
        return render_to_response('search.html', {'form': form, 'message': message},
                                  context_instance=RequestContext(request))

    search_args = {'after': str(form.cleaned_data['after']),
                   'before': str(form.cleaned_data['before']),
                   'project': form.cleaned_data['project']}

    search_entry_url = reverse(search_results, kwargs=search_args)
    params = {}
    for k in form.cleaned_data:
        if k in search_args.keys():
            continue
        params[k] = form.cleaned_data[k]
    return HttpResponseRedirect("%s?%s" % (search_entry_url, urlencode(params, True)))


def search_results(request, project, after, before):
    logger = logging.getLogger('telemetry').getChild('search-entry')
    logger.debug('Search after=%s, before=%s, parameters=%s', after, before, request.GET)
    after = UtcDateTime(after)
    before = UtcDateTime(before)
    conn = _aws_connection(project)
    obj_list = []
    event_count = 0
    email = request.GET.get('email', '')
    if email:
        clients = client_ids_from_email(email, after, before, project, include_url=False)
    else:
        clients = {}

    for event_type in request.GET.getlist('event_type', []):
        if event_type not in events.TYPES:
            msg = 'illegal event-type values %s' % request.GET.get('event_type')
            logger.error(msg)
            return render_to_response('search_results.html', {'message': msg},
                                      context_instance=RequestContext(request))

        query = Query()
        query.limit = 100000
        query.add('event_type', SelectorEqual(event_type))
        if after and before:
            query.add_range('uploaded_at', after, before)
        elif after:
            query.add('uploaded_at', SelectorGreaterThanEqual(after))
        elif before:
            query.add('uploaded_at', SelectorLessThanEqual(after))
        for k in request.GET:
            if k in ('event_type', 'email'):
                continue

            search = request.GET[k]
            if not search:
                continue  # don't allow empty values
            if k == 'message':
                if event_type == 'SUPPORT':
                    k = "support"
                elif event_type == 'CAPTURE':
                    k = "capture_name"
                elif event_type == 'COUNTER':
                    k = "counter_name"

            query.add(k, SelectorContains(search))

        try:
            logger.debug("Query=%s", query)
            (_obj_list, _event_count) = Monitor.query_events(conn, query, False, logger)
            if clients:
                _obj_list = [x for x in _obj_list if 'client' in x and x['client'] in clients.keys()]

            # TODO the count here seems wrong. It returns 0, despite the list being non-zero. For now, ignore the count.
            _event_count = len(_obj_list)
            logger.info('%d objects found', _event_count)
            if _obj_list:
                for o in _obj_list:
                    if 'message' not in o:
                        if 'support' in o:
                            o['message'] = o['support']
                        elif 'capture_name' in o:
                            o['message'] = o['capture_name']
                        elif 'counter_name' in o:
                            o['message'] = o['counter_name']

                obj_list.extend(_obj_list)
                event_count += _event_count
        except DynamoDBError, e:
            logger.error('failed to query events - %s', str(e))
            return render_to_response('search_results.html', {'message': 'failed to query events - %s' % str(e)},
                                      context_instance=RequestContext(request))

    for event in obj_list:
        event['url'] = reverse(entry_page, kwargs={'client': event['client'],
                                                   'timestamp': event['timestamp'],
                                                   'span': 1,
                                                   'project': project})
        event['class'] = event['event_type'].lower()

    params = [{'key': 'after', 'value': str(after)},
              {'key': 'before', 'value': str(before)},
              {'key': 'Events', 'value': event_count},
              ]
    for k in request.GET:
        if k == 'event_type':
            value = [str(x) for x in request.GET.getlist(k)]
        else:
            value = request.GET.get(k)
        params.append({'key': k, 'value': value})
    if clients:
        params.append({'key': 'clients (from email)', 'value': ", ".join(clients.keys())})

    return render_to_response('search_results.html', {'params': params,
                                                      'project': project,
                                                      'search_results': sorted(obj_list, reverse=True, key=lambda x: x['uploaded_at'])},
                              context_instance=RequestContext(request))


