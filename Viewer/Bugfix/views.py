import sys
#import pdb
import dateutil.parser
from datetime import timedelta
from django.http import HttpResponse
from django import forms
from django.shortcuts import render
from django.http import HttpResponseRedirect
#from pytz import timezone
import logging
import re
import cgi
import os
import tempfile


sys.path.append('../Parse/scripts')

import Parse

username = 'monitor'
api_key = 'FL6KXH6xFC2n4Y1pGQcpf0GWWX3FJ61GmdqZYY72'
app_id = 'uRVtTGj8WhhK4OJNRqKmSVg5FyS5gYXtQGIRRlqs'
default_span = 15


class LoginForm(forms.Form):
    password = forms.CharField(widget=forms.PasswordInput)


class VectorForm(forms.Form):
    tele_paste = forms.CharField(widget=forms.Textarea)


def _parse_crash_report(junk):
    timestamp = None
    device_id = None
    lines = junk.splitlines()
    for line in lines:
        splitty = line.split(':', 1)
        if 2 != len(splitty):
            continue
        key = splitty[0]
        value = splitty[1]
        if 'Device ID' == key:
            device_id = value.strip()
        elif 'Date/Time' == key:
            timestamp = value.strip()
    if timestamp and device_id:
        logger = logging.getLogger('telemetry').getChild('_parse_crash_report')
        logger.debug('timestamp=%s, device_id=%s', timestamp, device_id)
        return {'timestamp': timestamp, 'device_id': device_id}
    return None


def _parse_error_report(junk):
    timestamp = None
    client = None
    lines = junk.splitlines()
    for line in lines:
        splitty = line.split(':', 1)
        if 2 != len(splitty):
            continue
        key = splitty[0]
        value = splitty[1]
        if 'client' == key:
            client = value.strip()
        elif 'timestamp' == key:
            timestamp = value.strip()
    if timestamp and client:
        logger = logging.getLogger('telemetry').getChild('_parse_error_report')
        logger.debug('timestamp=%s, client=%d', timestamp, client)
        return {'timestamp': timestamp, 'client': client}
    return None

# Create your views here.


def login(request):
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
        else:
            logger.warn('invalid form data')
    form = LoginForm()
    return render(request, 'login.html', {'form': form})


def home(request):
    logger = logging.getLogger('telemetry').getChild('home')
    if not 'session_token' in request.session:
        logger.info('no session token. must log in first.')
        return HttpResponseRedirect('/login/')
    if request.method == 'POST':
        form = VectorForm(request.POST)
        if form.is_valid():
            logger.debug('tele_paste=%s', form.cleaned_data['tele_paste'])
            loc = _parse_error_report(form.cleaned_data['tele_paste'])
            if loc is not None:
                loc['span'] = str(default_span)
                return HttpResponseRedirect("/bugfix/logs/%(client)s/%(timestamp)s/(span)s/" % loc)
            loc = _parse_crash_report(form.cleaned_data['tele_paste'])
            if None != loc:
                conn = Parse.connection.Connection(app_id=app_id, api_key=api_key,
                                                   session_token=request.session['session_token'])
                query = Parse.query.Query()
                query.add('event_type', Parse.query.SelectorEqual('INFO'))
                search_key = 'Device ID: ' + str(loc['device_id'])
                query.add('message', Parse.query.SelectorStartsWith(search_key))
                events = Parse.query.Query.objects('Events', query, conn)[0]
                if len(events) != 0:
                    assert 'client' in events[0]
                    client = events[0]['client']
                    loc['client'] = client
                    loc['span'] = str(default_span)
                    logger.debug('client=%(client)s, span=%(span)s', loc)
                    return HttpResponseRedirect("/bugfix/logs/%(client)s/%(timestamp)s/%(span)s/" % loc)
            else:
                logger.warn('unable to parse pasted info.')
        else:
            logger.warn('invalid form data')
    form = VectorForm()
    return render(request, 'home.html', {'form': form})


def _iso_z_format(date):
    raw = date.isoformat()
    keep = raw.split('+', 1)[0]
    return keep + 'Z'


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
    try:
        obj_list = Parse.query.Query.objects('Events', query, conn)[0]
        logger.info('%d objects found', len(obj_list))
    except Parse.exception.ParseException, e:
        logger.error('fail to query events - %s', str(e))
    iso_center = _iso_z_format(center)
    iso_go_earlier = _iso_z_format(go_earlier)
    iso_go_later = _iso_z_format(go_later)

    # Just build a HTML page by hand
    def add_ctrl_button(text, url):
        return '<td><table style="border-collapse: collapse" border="1" cellpadding="5">' \
               '<td bgcolor="blue"><a href=%s><font color="white">%s</font></a></td></table><td>\n' % (url, text)

    def ctrl_url(client_, time_, span_):
        return '/bugfix/logs/%s/%s/%s/' % (client_, time_, span_)

    def add_summary_row(desc, value):
        return '  <tr><td>%s</td><td>%s</td></tr>\n' % (desc, value)

    # Add 3 buttons
    html = ''
    html += '<table><tr>\n'
    html += add_ctrl_button('Zoom out', ctrl_url(client, iso_center, span*2))
    html += add_ctrl_button('Go back %d min' % span, ctrl_url(client, iso_go_earlier, 2*span))
    html += add_ctrl_button('Go forward %d min' % span, ctrl_url(client, iso_go_later, 2*span))
    html += '</tr></table><br/>\n'

    # Add a summary table that describes some basic parameters
    html += '<table style="border-collapse: collapse" border="1" cellpadding="2"><str>\n'
    html += add_summary_row('Start Time (UTC)', after)
    html += add_summary_row('Stop Time (UTC)', before)
    html += add_summary_row('Client', client)
    html += add_summary_row('# Events', len(obj_list))
    if len(obj_list) > 0:
        html += add_summary_row('OS Type', obj_list[0]['os_type'])
        html += add_summary_row('OS Version', obj_list[0]['os_version'])
        html += add_summary_row('Device Model', obj_list[0]['device_model'])
        html += add_summary_row('Build Version', obj_list[0]['build_version'])
    html += '</table><br/>\n'

    if len(obj_list) > 0:
        # Add the events
        html += '<table style="border-collapse: collapse" border="1" cellpadding="2">\n'
        html += '<tr><th>Date (UTC)</th><th>Time (UTC)</th><th>Event Type</th><th>Field</th><th>Value</th></tr>\n'
        bg_colors = {'WARN': '#ffff99',
                     'ERROR': 'pink',
                     'WBXML_REQUEST': '#99ddff',
                     'WBXML_RESPONSE': '#99ddff',
                     'UI': 'lightgreen'}

        for event in obj_list:
            event_type = event['event_type']

            def beautify_iso8601(time):
                match = re.match('(?P<date>.+)T(?P<time>.+)Z', time)
                assert match
                return match.group('date'), match.group('time')

            def row_header(event_type_):
                # Set up row attributes
                attrs = 'align="left" valign="top"'
                if event_type in bg_colors:
                    attrs += ' bgcolor="%s"' % bg_colors[event_type_]
                header = '<tr %s>' % attrs
                return header

            def common_header(event_type_, iso, num_rows_):
                (date, time) = beautify_iso8601(iso)
                header = row_header(event_type_)
                # Date, time, and event type
                header += '<td rowspan="%d">%s</td><td rowspan="%d">%s</td><td rowspan="%d">%s</td>' % \
                          (num_rows_, date, num_rows_, time, num_rows_, event_type_.replace('_', ' '))
                return header

            # Event type specific processing
            if event_type in ['DEBUG', 'INFO', 'WARN', 'ERROR']:
                html += common_header(event_type, event['timestamp']['iso'], 1)
                html += '<td>message</td><td>%s</td>' % cgi.escape(event['message'])
            elif event_type in ['WBXML_REQUEST', 'WBXML_RESPONSE']:
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
                wbxml = decode_wbxml(event['wbxml']['base64'])
                html += common_header(event_type, event['timestamp']['iso'], 1)
                html += '<td>wbxml</td><td><pre>%s</pre></td>' % cgi.escape(wbxml)
            elif event_type == 'UI':
                num_rows = 2  # for ui_type, ui_object
                if 'ui_string' in event:
                    num_rows += 1
                if 'ui_integer' in event:
                    num_rows += 1
                tr = row_header(event_type)
                html += common_header(event_type, event['timestamp']['iso'], num_rows)
                html += '<td>ui_type</td><td>%s</td></tr>\n' % event['ui_type']
                html += tr + '<td>ui_object</td><td>%s</td>' % event['ui_object']
                if 'ui_string' in event:
                    html += '</tr>\n' + tr + '<td>ui_string</td><td>%s</td>' % event['ui_string']
                if 'ui_integer' in event:
                    html += '</tr>\n' + tr + '<td>ui_integer</td><td>%s</td>' % event['ui_integer']
            else:
                logger.warn('unknown handled event type %s', event_type)
                html += common_header(event_type, event['timestamp']['iso'], 1)
            html += '</tr>\n'
        html += '</table>\n'
    return HttpResponse(html)
