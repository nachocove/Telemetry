import sys
import dateutil.parser
from datetime import timedelta
from django.http import HttpResponse
from django import forms
from django.shortcuts import render
from django.http import HttpResponseRedirect
import logging
import cgi
import os
import tempfile
import json
import ConfigParser

sys.path.append('../Parse/scripts')

import Parse
from monitor_base import Monitor
from support import Support


username = 'monitor'

# Get the list of project
projects = ConfigParser.ConfigParser()
projects.read('projects.cfg')

# Get the project name from the environment variable
project = os.getenv('PROJECT')
api_key = None
app_id = None
if not project:
    project = projects.sections()[0]
    api_key = projects.get(project, 'api_key')
    app_id = projects.get(project, 'app_id')
else:
    for p in projects.sections():
        if p == project:
            api_key = projects.get(p, 'api_key')
            app_id = projects.get(p, 'app_id')
assert api_key and app_id
logger = logging.getLogger('telemetry')
logger.info('project = %s', project)
logger.info('api_key = %s', api_key)
logger.info('app_id = %s', app_id)

default_span = 15


class LoginForm(forms.Form):
    password = forms.CharField(widget=forms.PasswordInput)


class VectorForm(forms.Form):
    tele_paste = forms.CharField(widget=forms.Textarea)


def _parse_junk(junk, mapping):
    retval = dict()
    lines = junk.splitlines()
    for line in lines:
        splitty = line.split(':', 1)
        if 2 != len(splitty):
            continue
        key = splitty[0]
        value = splitty[1]
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

# Create your views here.


def login(request):
    message = ''
    logger = logging.getLogger('telemetry').getChild('login')
    if request.method == 'POST':
        form = LoginForm(request.POST)
        if form.is_valid():
            logger.debug('app_id=%s', app_id)
            logger.debug('api_key=%s', api_key)
            conn = Parse.connection.Connection(app_id=app_id, api_key=api_key)
            try:
                user = Parse.users.User.login(username=username, password=form.cleaned_data['password'], conn=conn)
                request.session['session_token'] = user.session_token
                logger.debug('session_token=%s' % user.session_token)
                return HttpResponseRedirect('/')
            except Parse.exception.ParseException, e:
                logger.error('fail to get session token - %s' % str(e))
                message = 'Cannot log in (%s). Please enter the password again.' % e.error
        else:
            logger.warn('invalid form data')
    form = LoginForm()
    return render(request, 'login.html', {'form': form, 'message': message, 'project': project})


def home(request):
    logger = logging.getLogger('telemetry').getChild('home')
    if not 'session_token' in request.session:
        logger.info('no session token. must log in first.')
        return HttpResponseRedirect('/login/')
    # Any message set in 'message' will be displayed as a red error message.
    # Used for reporting error in any POST.
    message = ''
    if request.method == 'POST':
        form = VectorForm(request.POST)
        if form.is_valid():
            logger.debug('tele_paste=%s', form.cleaned_data['tele_paste'])
            loc = _parse_error_report(form.cleaned_data['tele_paste'])
            if loc is not None:
                loc['span'] = str(default_span)
                return HttpResponseRedirect("/bugfix/logs/%(client)s/%(timestamp)s/%(span)s/" % loc)
            loc = _parse_support_email(form.cleaned_data['tele_paste'])
            if loc is not None:
                loc['span'] = str(default_span)
                # From the email, we need to find the Parse client ID
                conn = Parse.connection.Connection(app_id=app_id, api_key=api_key,
                                                   session_token=request.session['session_token'])
                query = Parse.query.Query()
                query.add('event_type', Parse.query.SelectorEqual('SUPPORT'))
                events = Parse.query.Query.objects('Events', query, conn)[0]
                email_events = Support.get_sha256_email_address(events, loc['email'])[1]
                if len(email_events) != 0:
                    loc['client'] = email_events[-1].client
                    if 'timestamp' not in loc:
                        loc['timestamp'] = email_events[-1].timestamp
                    loc['span'] = str(default_span)
                    logger.debug('client=%(client)s, span=%(span)s', loc)
                    return HttpResponseRedirect("/bugfix/logs/%(client)s/%(timestamp)s/%(span)s/" % loc)
                else:
                    message = 'Cannot find client ID for email %s' % loc['email']
                    logger.warn(message)
            loc = _parse_crash_report(form.cleaned_data['tele_paste'])
            if None != loc:
                # From the crash log device ID, we need to find the Parse client ID
                conn = Parse.connection.Connection(app_id=app_id, api_key=api_key,
                                                   session_token=request.session['session_token'])
                query = Parse.query.Query()
                query.add('event_type', Parse.query.SelectorEqual('INFO'))
                search_key = 'Device ID: ' + str(loc['device_id'])
                query.add('message', Parse.query.SelectorStartsWith(search_key))
                events = Parse.query.Query.objects('Events', query, conn)[0]
                if len(events) != 0:
                    assert 'client' in events[-1]
                    client = events[-1]['client']
                    loc['client'] = client
                    loc['span'] = str(default_span)
                    logger.debug('client=%(client)s, span=%(span)s', loc)
                    return HttpResponseRedirect("/bugfix/logs/%(client)s/%(timestamp)s/%(span)s/" % loc)
                else:
                    message = 'Cannot find client ID for device ID %s' % loc['device_id']
                    logger.warn(message)
            else:
                logger.warn('unable to parse pasted info.')
        else:
            logger.warn('invalid form data')
    form = VectorForm()
    return render(request, 'home.html', {'form': form, 'message': message, 'project': project})


def _iso_z_format(date):
    raw = date.isoformat()
    keep = raw.split('+', 1)[0]
    return keep[:-3] + 'Z'


def entry_page(request, client='', timestamp='', span=str(default_span)):
    logger = logging.getLogger('telemetry').getChild('entry_page')
    if not 'session_token' in request.session:
        logger.info('no session token. must log in first.')
        return HttpResponseRedirect('/login/')

    logger.info('client=%s, timestamp=%s, span=%s', client, timestamp, span)
    span = int(span)
    client = str(client)
    center = dateutil.parser.parse(timestamp)
    spread = timedelta(minutes=int(span))
    after = center - spread
    before = center + spread
    go_earlier = after - spread
    go_later = before + spread
    conn = Parse.connection.Connection(app_id=app_id, api_key=api_key,
                                       session_token=request.session['session_token'])
    query = Parse.query.Query()
    query.limit = 1000
    query.skip = 0
    query.add('client', Parse.query.SelectorEqual(client))
    #return HttpResponse(before)
    query.add('timestamp', Parse.query.SelectorGreaterThanEqual(Parse.utc_datetime.UtcDateTime(str(after))))
    query.add('timestamp', Parse.query.SelectorLessThan(Parse.utc_datetime.UtcDateTime(str(before))))
    logger.debug('query=%s', str(query.where()))
    obj_list = list()
    event_count = 0
    try:
        (obj_list, event_count) = Monitor.query_events(conn, query, False, logger)
        logger.info('%d objects found', len(obj_list))
    except Parse.exception.ParseException, e:
        logger.error('fail to query events - %s', str(e))
    iso_center = _iso_z_format(center)
    iso_go_earlier = _iso_z_format(go_earlier)
    iso_go_later = _iso_z_format(go_later)

    # Just build a HTML page by hand
    def add_ctrl_button(text, url):
        return '<td><table class="button_table">' \
               '<td class="button_cell"><a href=%s><font class="button_text">%s</font></a></td></table><td>\n' % \
               (url, text)

    def ctrl_url(client_, time_, span_):
        return '/bugfix/logs/%s/%s/%s/' % (client_, time_, span_)

    # Set up style sheet
    html = '<link rel="stylesheet" type="text/css" href="/static/list.css">\n'

    # Save some global parameters for summary table
    html += '<script type="text/javascript">\n'
    params = dict()
    params['start'] = after.isoformat('T')
    params['stop'] = before.isoformat('T')
    params['client'] = client
    params['event_count'] = event_count
    if len(obj_list) > 0:
        params['os_type'] = obj_list[0]['os_type']
        params['os_version'] = obj_list[0]['os_version']
        params['device_model'] = obj_list[0]['device_model']
        params['build_version'] = obj_list[0]['build_version']
    html += 'var params = ' + json.dumps(params) + ';\n'

    # Generate the events JSON
    for event in obj_list:
        event['timestamp'] = event['timestamp']['iso']
        for field in ['createdAt', 'updatedAt', 'os_type', 'os_version', 'device_model', 'build_version']:
            del event[field]

        if event['event_type'] in ['WBXML_REQUEST', 'WBXML_RESPONSE']:
            def decode_wbxml(wbxml_):
                # This is kind ugly. A better looking solution would involve
                # using subprocess but redirecting pipes causes my Mac to
                # crash! I'm guessing it has something to do with redirecting
                # stdin / stdout in a WSGI process. Regardless to aesthetic,
                # this solution works fine.
                path = tempfile.mktemp()
                os.system('mono %s -d -b %s > %s' %
                          (os.path.realpath('./WbxmlTool.Mac.exe'), wbxml_, path))
                with open(path, 'r') as f:
                    output = f.read()
                os.unlink(path)
                return output
            base64 = event['wbxml']['base64']
            event['wbxml_base64'] = cgi.escape(base64)
            event['wbxml'] = cgi.escape(decode_wbxml(base64))
        if 'message' in event:
            event['message'] = cgi.escape(event['message'])

    html += 'var events = ' + json.dumps(obj_list) + ';\n'
    html += '</script>\n'
    html += '<script type="text/javascript" src="/static/list.js"></script>\n'

    # Add 3 buttons
    html += '<body onload="refresh()">\n'
    html += '<h1>%s</h1><hr>' % project
    html += '<table><tr>\n'
    zoom_in_span = max(1, span/2)
    html += add_ctrl_button('Zoom in (%d min)' % zoom_in_span, ctrl_url(client, iso_center, zoom_in_span))
    html += add_ctrl_button('Zoom out (%d min)' % (span*2), ctrl_url(client, iso_center, span*2))
    html += add_ctrl_button('Go back %d min' % (2*span), ctrl_url(client, iso_go_earlier, span))
    html += add_ctrl_button('Go forward %d min' % (2*span), ctrl_url(client, iso_go_later, span))
    html += '</tr></table><br/>\n'

    # Add a summary table that describes some basic parameters of the query
    html += '<table id="table_summary" class="table"></table><br/>\n'

    # Add an event table
    html += '<table id="table_events" class="table"></table>\n'
    html += '</body>\n'

    return HttpResponse(html)
