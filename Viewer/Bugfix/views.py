import sys
#import pdb
import dateutil.parser
#import subprocess
from datetime import timedelta
#from django.shortcuts import render
from django.http import HttpResponse
#from django import template
from django.template.loader import get_template
from django.template import Context
from django import forms
from django.shortcuts import render
from django.http import HttpResponseRedirect
#from pytz import timezone
import logging

sys.path.append('../Parse/scripts')

username = 'monitor'
api_key = 'FL6KXH6xFC2n4Y1pGQcpf0GWWX3FJ61GmdqZYY72'
app_id = 'uRVtTGj8WhhK4OJNRqKmSVg5FyS5gYXtQGIRRlqs'
default_span = 15

import Parse


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
    form = LoginForm()
    return render(request, 'login.html', {'form': form})


def home(request):
    if not 'session_token' in request.session:
        return HttpResponseRedirect('/login/')
    if request.method == 'POST':
        form = VectorForm(request.POST)
        if form.is_valid():
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
                    return HttpResponseRedirect("/bugfix/logs/%(client)s/%(timestamp)s/%(span)s/" % loc)
    form = VectorForm()
    return render(request, 'home.html', {'form': form})


def _iso_z_format(date):
    raw = date.isoformat()
    keep = raw.split('+', 1)[0]
    return keep + 'Z'


def entry_page(request, client='', timestamp='', span=str(default_span)):
    if not 'session_token' in request.session:
        return HttpResponseRedirect('/login/')
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
    query.limit = 5000
    query.skip = 0
    query.add('client', Parse.query.SelectorEqual(client))
    #return HttpResponse(before)
    query.add('timestamp', Parse.query.SelectorGreaterThanEqual(Parse.utc_datetime.UtcDateTime(str(after))))
    query.add('timestamp', Parse.query.SelectorLessThan(Parse.utc_datetime.UtcDateTime(str(before))))
    obj_list = Parse.query.Query.objects('Events', query, conn)[0]
    iso_center = _iso_z_format(center)
    iso_go_earlier = _iso_z_format(go_earlier)
    iso_go_later = _iso_z_format(go_later)
    ctxt = Context({'obj_list': obj_list,
                    'client': client,
                    'center': iso_center,
                    'go_earlier': iso_go_earlier,
                    'go_later': iso_go_later,
                    'span': span,
                    'zoom': span*2})
    tpl = get_template('list.html')
    return HttpResponse(tpl.render(ctxt))
