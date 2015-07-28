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
from boto.s3.connection import S3Connection
from AWS.s3t3_telemetry import get_client_events,  T3_EVENT_CLASS_FILE_PREFIXES, get_pinger_events, get_latest_device_info_event
from core.auth import nacho_cache, nachotoken_required

from PyWBXMLDecoder.ASCommandResponse import ASCommandResponse
from misc.utc_datetime import UtcDateTime

T3_TYPES = ['ALL',
         'DEBUG',
         'INFO',
         'WARN',
         'ERROR',
         'WBXML_REQUEST',
         'WBXML_RESPONSE',
         'IMAP_REQUEST',
         'IMAP_RESPONSE',
         'COUNTER',
         'STATISTICS2',
         'UI',
         'SUPPORT',
         'DISTRIBUTION',
         'SAMPLES'
         'TIMESERIES',
         'DEVICEINFO',
         'PINGER']

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
        if password == "nacho1234":
            self.add_error('password', 'Incorrect Password')
        return password

class VectorForm(forms.Form):
    project = forms.ChoiceField(choices=[(x, x.capitalize()) for x in projects])
    event_class = forms.ChoiceField(choices=[(x, x.capitalize()) for x in sorted(T3_EVENT_CLASS_FILE_PREFIXES)])
    tele_paste = forms.CharField(widget=forms.Textarea)

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
    if 'timestamp' not in retval: # default to timestamp now
        retval['timestamp'] = _iso_z_format(datetime.utcnow())
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

def parse_paste_data(junk):
    dict_ = _parse_junk(junk, {'timestamp': 'timestamp',
                                'userid': 'userid',
                                'user-id': 'userid',
                                'User ID': 'userid',
                                'deviceid': 'deviceid',
                                'Device ID': 'deviceid',
                                'Date/Time': 'timestamp',
                                'Launch Time': 'timestamp',
                                'span': 'span',
                                'email': 'email',
                                'search': 'search',
                                'threadid' : 'threadid'
                               })
    if 'timestamp' in dict_:
        if dict_['timestamp'] == 'now':
            dict_['timestamp'] = _iso_z_format(datetime.utcnow())
        else:
            dict_['timestamp'] = UtcDateTime(dict_['timestamp'])
        return dict_
    return None


def process_report(request, project, form, loc, logger):
    kwargs = {'timestamp': loc['timestamp'],
                'span': loc.get('span', default_span),
                'project': project}
    if 'userid' in loc:
        kwargs['userid'] = loc['userid']
    if 'deviceid' in loc:
        kwargs['deviceid'] = loc['deviceid']
    if 'search' in loc:
        kwargs['search'] = loc['search']
    if 'threadid' in loc:
        kwargs['threadid'] = loc['threadid']
    else:
        kwargs['threadid'] = 0
    if 'event_class' in loc:
        kwargs['event_class'] = loc['event_class']
    else:
        kwargs['event_class'] = 'ALL'
    if 'userid' not in loc and 'deviceid' not in loc and 'email' in loc:
        device_list = get_device_list_from_email(project, loc['timestamp'], loc['email'],
                                                    loc.get('span', default_span), loc['event_class'])
        if device_list:
            if len(device_list) > 1:
                return render_to_response('device_picker.html', {'device_list': device_list.values(), 'project': project, 'email': loc['email']},
                                  context_instance=RequestContext(request))
            elif len(device_list) == 1:
                kwargs['deviceid'] = device_list[device_list.keys()[0]]['device_id']
    return HttpResponseRedirect(reverse(entry_page, kwargs=kwargs))

def obfuscate_email(email_address):
    index = email_address.find('@')
    if 0 > index:
        raise ValueError('Invalid email address')
    if index != email_address.rfind('@'):
        raise ValueError('Invalid email address')
    email, domain = email_address.split('@')
    return "%s@%s" % (hashlib.sha256(email.lower()).hexdigest(), domain)

def get_filtered_events(events, filter):
    filtered_events=[]
    for ev in events:
        if 'sha256_email_address' in ev['support']:
            support = json.loads(ev['support'])
            if support['sha256_email_address'] == filter:
                filtered_events.append(ev)
    return filtered_events

def get_email_address_events(email_address, support_events):
    email, domain = email_address.split('@')
    obfuscated = obfuscate_email(email_address)
    if email:
        filtered_events = get_filtered_events(support_events, obfuscated)
        if len(filtered_events) == 0 and len(email) == 64:
            # perhaps the email given is already an obfuscated one? Let's try it.
            filtered_events = get_filtered_events(support_events, email_address)
    elif domain:
        filtered_events=[]
        for ev in support_events:
            if 'sha256_email_address' in ev['support']:
                support = json.loads(ev['support'])
                if support['sha256_email_address'].endswith('@'+domain):
                    filtered_events.append(ev)
        obfuscated = None
    else:
        raise Exception("Bad email %s", email_address)
    return obfuscated, filtered_events

def get_device_list(email_events, project, span, event_class):
    device_list = {}
    for ev in email_events:
        key = ev['user_id'] + ':' + ev['device_id']
        url = reverse(entry_page, kwargs={'userid': ev['user_id'],
                                          'deviceid': ev['device_id'],
                                          'timestamp': ev['timestamp'],
                                          'span': span,
                                          'event_class': event_class,
                                          'project': project})
        if key not in device_list:
            device_data = {}
            device_data['user_id'] = ev['user_id']
            device_data['device_id'] = ev['device_id']
            support = json.loads(ev['support'])
            device_data['sha_email'] = support['sha256_email_address']
            device_data['first_timestamp'] = ev['timestamp']
            device_data['last_timestamp'] = ev['timestamp']
            device_data['url'] = url
            device_list[key] = device_data
        else:
            device_list[key]['last_timestamp'] = ev['timestamp']
            device_list[key]['url'] = url
    return device_list

def get_device_list_from_email(project, timestamp, email_address, span, event_class):
    logger = logging.getLogger('telemetry').getChild('client_telemetry')
    # search a day back from timestamp backwards, 7 times
    before = UtcDateTime(str(timestamp))
    for i in range(7):
        from datetime import timedelta
        after = before.datetime - timedelta(days=1)
        after = UtcDateTime(str(after))
        logger.info("Check from %s to %s for support events", after, before)
        support_events = get_support_events(project, after, before, logger=logger)
        obfuscated, email_events = get_email_address_events(email_address, support_events)
        if len(email_events) > 0:
            return get_device_list(email_events, project, span, event_class)
        before = UtcDateTime(str(after))
    return None

def get_support_events(project, after, before, logger=None):
    conn = _aws_s3_connection(project)
    bucket_name = projects_cfg.get(project, 'client_t3_support_bucket')
    support_event_list = get_client_events(conn, bucket_name, '', '', after, before, 'SUPPORT', '', logger=logger)
    return support_event_list

@nachotoken_required
def index(request):
    logger = logging.getLogger('telemetry').getChild('index')
    # Any message set in 'message' will be displayed as a red error message.
    # Used for reporting error in any POST.
    message = ''
    return render_to_response('index.html', {'message': message},
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
    event_class = form.cleaned_data['event_class']
    request.session['project'] = project
    request.session['event_class'] = event_class

    paste_data = form.cleaned_data['tele_paste']

    loc = parse_paste_data(paste_data)
    if loc is not None:
        loc['event_class'] = event_class
        return process_report(request, project, form, loc, logger)

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

def ctrl_url(userid, deviceid, event_class, search, threadid, time, span, project):
    kwargs = {'timestamp': time,
                'span': span,
                'project': project}
    if userid != '':
        kwargs['userid'] = userid
    if deviceid != '':
        kwargs['deviceid'] = deviceid
    if event_class != '':
        kwargs['event_class'] = event_class
    else:
        kwargs['event_class'] = 'ALL'
    if search != '':
        kwargs['search'] = search
    kwargs['threadid'] = threadid
    return reverse(entry_page, kwargs=kwargs)

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
def entry_page(request, project='', userid='', deviceid='', event_class='ALL', search='', threadid=0, timestamp='', span=str(default_span)):
    logger = logging.getLogger('telemetry').getChild('entry_page')
    logger.info('userid=%s, deviceid=%s, event_class=%s search=%s threadid=%s timestamp=%s, span=%s', userid, deviceid, event_class, search, threadid, timestamp, span)
    span = int(span)
    userid = str(userid)
    deviceid = str(deviceid)
    threadid = int(threadid)
    if timestamp.strip() == 'now':
        center = _iso_z_format(datetime.utcnow())
        response =  HttpResponseRedirect(ctrl_url(userid, deviceid, event_class, search, threadid, center, span, project))
        from django.utils.cache import add_never_cache_headers
        add_never_cache_headers(response)
        return response
    else:
        center = dateutil.parser.parse(timestamp)
    spread = timedelta(minutes=int(span))
    after = center - spread
    before = center + spread

    context = entry_page_base(project, userid, deviceid, event_class, search, threadid, after, before, request.GET, logger)

    iso_go_earlier, iso_center, iso_go_later = calc_spread(after, before, span=span, center=center)

    # Add buttons
    context['buttons'] = []
    zoom_in_span = max(1, span/2)
    context['buttons'].append({'text': 'Zoom in (%d min)' % zoom_in_span,
                               'url': ctrl_url(userid, deviceid, event_class, search, threadid, iso_center, zoom_in_span, project),
                               })
    context['buttons'].append({'text': 'Zoom out (%d min)' % (span*2),
                               'url': ctrl_url(userid, deviceid, event_class, search, threadid, iso_center, span*2, project),
                               })
    context['buttons'].append({'text': 'Go back %d min' % span,
                               'url': ctrl_url(userid, deviceid, event_class, search, threadid, iso_go_earlier, span, project),
                               })
    context['buttons'].append({'text': 'Go forward %d min' % span,
                               'url': ctrl_url(userid, deviceid, event_class, search, threadid, iso_go_later, span, project),
                               })
    context['buttons'].append({'text': 'Go to now',
                               'url': ctrl_url(userid, deviceid, event_class, search, threadid, 'now', span, project),
                               })
    context['body_args'] = 'onload=refresh()'
    return render_to_response('entry_page.html', context,
                              context_instance=RequestContext(request))

@nachotoken_required
@nacho_cache
def entry_page_by_timestamps(request, project, userid='', deviceid='', event_class='ALL', search='', threadid=0, after='', before=''):
    logger = logging.getLogger('telemetry').getChild('entry_page')
    logger.info('userid=%s, deviceid=%s, event_class=%s, search=%s, threadid=%s, after=%s, before=%s', userid, deviceid, event_class, search, threadid, after, before)
    context = entry_page_base(project, userid, deviceid, event_class, search, threadid, after, before, request.GET, logger)
    iso_go_earlier, iso_center, iso_go_later = calc_spread(after, before, span=default_span, center=None)
    context['buttons'] = []
    zoom_in_span = max(1, default_span/2)
    context['buttons'].append({'text': 'Zoom in (%d min)' % zoom_in_span,
                               'url': ctrl_url(userid, deviceid, event_class, search, threadid, iso_center, zoom_in_span, project),
                               })
    context['buttons'].append({'text': 'Zoom out (%d min)' % (default_span*2),
                               'url': ctrl_url(userid, deviceid, event_class, search, threadid, iso_center, default_span*2, project),
                               })
    context['buttons'].append({'text': 'Go back %d min' % default_span,
                               'url': ctrl_url(userid, deviceid, event_class, search, threadid, iso_go_earlier, default_span, project),
                               })
    context['buttons'].append({'text': 'Go forward %d min' % default_span,
                               'url': ctrl_url(userid, deviceid, event_class, search, threadid, iso_go_later, default_span, project),
                               })
    context['body_args'] = 'onload=refresh()'
    return render_to_response('entry_page.html', context,
                              context_instance=RequestContext(request))

def get_pinger_telemetry(project, conn, userid, deviceid, after, before, search):
    logger = logging.getLogger('telemetry').getChild('pinger_telemetry')
    bucket_name = projects_cfg.get(project, 'pinger_telemetry_bucket')
    some_events = get_pinger_events(conn, bucket_name, userid, deviceid, after, before, search, logger=logger)
    events = []
    for ev in some_events:
        # TODO - what's this for?
        if 'client' in ev:
            if userid and userid != ev['client']:
                continue
            parts = ev['client'].split(':')
            if len(parts) > 2:
                ev['client'] = ":".join(parts[0:2])
        events.append(ev)
    return events

def get_t3_events(project, userid, deviceid, event_class, search, threadid, after, before):
    logger = logging.getLogger('telemetry').getChild('client_telemetry')
    conn = _aws_s3_connection(project)
    event_classes = T3_EVENT_CLASS_FILE_PREFIXES[event_class]
    if isinstance(event_classes, list):
        all_events = []
        for ev_class in event_classes:
            if ev_class == 'PINGER':
               some_events = get_pinger_telemetry(project, conn, userid, deviceid, after, before, search)
            else:
                bucket_name = projects_cfg.get(project, 'client_t3_%s_bucket' % T3_EVENT_CLASS_FILE_PREFIXES[ev_class])
                some_events = get_client_events(conn, bucket_name, userid, deviceid, after, before, ev_class, search, threadid, logger=logger)
            all_events.extend(some_events)
    else:
        if event_class == 'PINGER':
            all_events = get_pinger_telemetry(project, conn, userid, deviceid, after, before, search)
        else:
            bucket_name = projects_cfg.get(project, 'client_t3_%s_bucket' % T3_EVENT_CLASS_FILE_PREFIXES[event_class])
            all_events = get_client_events(conn, bucket_name, userid, deviceid, after, before, event_class, search, threadid, logger=logger)
    all_events = sorted(all_events, key=lambda x: x['timestamp'])
    return all_events

def get_last_device_info_event(project, userid, deviceid, after, before):
    logger = logging.getLogger('telemetry').getChild('client_telemetry')
    conn = _aws_s3_connection(project)
    bucket_name = projects_cfg.get(project, 'client_t3_device_info_bucket')
    return get_latest_device_info_event(conn, bucket_name, userid, deviceid, after, before, logger=logger)

def entry_page_base(project, userid, deviceid, event_class, search, threadid, after, before, params, logger):
    event_list = []
    userid_list = []
    if userid != '' or deviceid != '':
        last_device_info_event = get_last_device_info_event(project, userid, deviceid, UtcDateTime(str(after)), UtcDateTime(str(before)))
    else:
        last_device_info_event = None
    if userid == '' and deviceid != '' and last_device_info_event:
        userid = last_device_info_event['user_id']
        logger.info("getting User ID %s from last device info" % userid)
    if deviceid == '' and userid != '' and last_device_info_event:
        deviceid = last_device_info_event['device_id']
        logger.info("getting Device ID %s from last device info" % deviceid)
    event_list = get_t3_events(project, userid, deviceid, event_class, search, threadid, UtcDateTime(str(after)), UtcDateTime(str(before)))

    # Save some global parameters for summary table
    params = dict()
    params['start'] = after
    params['stop'] = before
    params['event_count'] = len(event_list)

    # get latest device info
    if last_device_info_event and event_class != 'PINGER':
        # Get the data from the LAST client-entry
        params.update(last_device_info_event)

    # Generate the events JSON
    omit_list = ['uploaded_at',]
    if userid:
        omit_list.append('userid')

    for event in event_list:
        for field in omit_list:
            if field in event:
                del event[field]

        if event['event_type'] in ['WBXML_REQUEST', 'WBXML_RESPONSE']:
            def decode_wbxml(wbxml_):
                instance = ASCommandResponse(base64.b64decode(wbxml_))
                return '\n'.join(instance.xmlString.split('\n')[1:])

            b64 = event['payload'].encode()
            event['wbxml_base64'] = cgi.escape(b64)
            event['wbxml'] = cgi.escape(decode_wbxml(b64))
        elif event['event_type'] in ['IMAP_REQUEST', 'IMAP_RESPONSE']:
            event['message'] = base64.b64decode(event['payload'])
            del event['payload']

        if 'message' in event:
            event['message'] = cgi.escape(event['message'])
        if 'timestamp' in event:
            event['timestamp'] = str(event['timestamp'])
        if not userid and event.get('userid', ''):
            #TODO fix this
            event['userid'] = '<a href=%s target="_blank">%s</a>' % (ctrl_url(event['userid'], event['timestamp'], default_span, project), event['userid'])
    context = {'project': project,
                'params': json.dumps(params, default=json_formatter),
                'events': json.dumps(event_list, default=json_formatter),
                'show_user_id': 0 if userid else 1,
                'show_device_id': 0 if deviceid else 1,
               }

    return context

def event_choices():
    ec = {}
    for ev in T3_TYPES:
        if ev in ('WBXML_REQUEST', 'WBXML_RESPONSE'):
            continue  # omit these
        elif ev in ('INFO', 'DEBUG'):
            ec[ev] = ev.lower().capitalize() + " (DANGER: Lots of records!)"
        elif ev in ('UI',):
            continue
        else:
            ec[ev] = ev.lower().capitalize()
    return sorted([ (k, ec[k]) for k in ec ])
