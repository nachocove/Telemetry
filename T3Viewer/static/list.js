var isUtc = true;
var SWITCH_TO_UTC = 'Click to switch to UTC time';
var SWITCH_TO_LOCAL = 'Click to switch to local time';
var DATE_UTC = 'Date (UTC)';
var TIME_UTC = 'Time (UTC)';
var DATE_LOCAL = 'Date (Local)';
var TIME_LOCAL = 'Time (Local)';
var START_TIME_UTC = 'Start Time (UTC)';
var STOP_TIME_UTC = 'Stop Time (UTC)';
var START_TIME_LOCAL = 'Start Time (Local)';
var STOP_TIME_LOCAL = 'Stop Time (Local)';
var START_FIELD_ID = 'start_field';
var START_VALUE_ID = 'start_value';
var STOP_FIELD_ID = 'stop_field';
var STOP_VALUE_ID = 'stop_value';

function zeroPad(s, width) {
    out = s.toString();
    while (out.length < width) {
        out = '0' + out;
    }
    return out;
}

function zeroPad2(s) {
    return zeroPad(s, 2);
}

function dateUtc(date) {
    return date.getUTCFullYear() + '-' + zeroPad2(date.getUTCMonth()+1) + '-' + zeroPad2(date.getUTCDate());
}

function timeUtc(date) {
    return (zeroPad2(date.getUTCHours()) + ':' + zeroPad2(date.getUTCMinutes()) + ':' +
            zeroPad2(date.getUTCSeconds()) + '.' + zeroPad(date.getUTCMilliseconds(), 3));
}

function dateLocal(date) {
    return date.getFullYear() + '-' + zeroPad2(date.getMonth()+1) + '-' + zeroPad2(date.getDate());
}

function timeLocal(date) {
    return (zeroPad2(date.getHours()) + ':' + zeroPad2(date.getMinutes()) + ':' +
            zeroPad2(date.getSeconds()) + '.' + zeroPad(date.getMilliseconds(), 3));
}

function dateTimeUtc(iso) {
    date = new Date(iso);
    return dateUtc(date) + ' ' + timeUtc(date);
}

function dateTimeLocal(iso) {
    date = new Date(iso);
    return dateLocal(date) + ' ' + timeLocal(date);
}

function getRow(event) {
    var tr = document.createElement('tr');

    // Set up the style for the row
    var event_type;
    switch (event.event_type) {
        case 'WBXML_REQUEST':
        case 'WBXML_RESPONSE':
            event_type = 'wbxml';
            break;
        case 'IMAP_REQUEST':
        case 'IMAP_RESPONSE':
            event_type = 'imap';
            break;
        default:
            event_type = event.event_type.toLowerCase();
            break;
    }
    tr.className = event_type;
    tr.id = event.id;
    return tr;
}

function getCell(html, rowSpan) {
    if (typeof rowSpan == 'undefined') {
        rowSpan = 1; // default is 1 row
    }
    var td = document.createElement('td');
    td.className = 'cell';
    td.innerHTML = html;
    td.rowSpan = rowSpan;
    return td;
}

function getRowWithCommonFields (id, event, num_rows) {
    var tr = getRow(event);
    iso = new Date(event.timestamp);

    date = getCell(dateUtc(iso), num_rows);
    date.id = 'date_' + id;
    tr.appendChild(date);

    time = getCell(timeUtc(iso), num_rows);
    time.id = 'time_' + id;
    tr.appendChild(time);

    tr.appendChild(getCell(event.event_type.replace('_', ' '), num_rows));
    id_cell = getCell(event.id, num_rows)
    id_cell.className += " id_cell"
    tr.appendChild(id_cell);

    if (show_user_id == 1) {
        tr.appendChild(getCell(event.user_id, num_rows));
    }

    if (show_device_id == 1) {
        tr.appendChild(getCell(event.device_id, num_rows));
    }
    tr.appendChild(getCell(event.module.replace('_', ' '), num_rows));
    if (event.hasOwnProperty('thread_id'))  {
        tr.appendChild(getCell('TID-' + event.thread_id, num_rows));
    }
    else {
            tr.appendChild(getCell('', num_rows));
    }
    return tr;
}

function getDummyRow (type, module, message) {
    var tr = document.createElement('tr');
    tr.className = type.toLowerCase();
    tr.id = 0;
    // date
    var c = getCell("", 1);
    tr.appendChild(c);
    // time
    c = getCell("", 1);
    tr.appendChild(c);
    // TYPE
    c = getCell(type, 1);
    tr.appendChild(c);
    // module
    c = getCell(module);
    tr.appendChild(c);
    // tid
    c = getCell("", 1);
    tr.appendChild(c);
    // field
    c = getCell("message", 1);
    tr.appendChild(c);
    // message
    c = getCell(message, 1);
    tr.appendChild(c);
    return tr;
}
function getPre(html) {
    return '<pre>' + html + '</pre>';
}

function addFieldToRow(row, field, value) {
    row.appendChild(getCell(field));
    var valueCell = getCell(value);
    row.appendChild(valueCell);
    return valueCell;
}

function addSummaryRow(table, field, value) {
    var tr = document.createElement('tr');
    tr.appendChild(getCell(field));
    tr.appendChild(getCell(value));
    table.appendChild(tr);
}

function htmlUnescape(s) {
    // not robust but good enough for our use.
    return s.replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&');
}

function previewString(s) {
    var N = 20;
    if (s.length <= N) {
        return s;
    }
    var i = s.indexOf("\n")
    return htmlUnescape(s.slice(0, Math.min(i, N))) + '...';
}

function isElementInViewport(e) {
     var rect = e.getBoundingClientRect();
     return (rect.bottom > 0 &&
             rect.right > 0 &&
             rect.left < (window.innerWidth || document. documentElement.clientWidth) &&
             rect.top < (window.innerHeight || document. documentElement.clientHeight));
 }

function beautifyBase64(b64) {
    var N = 512;
    var out = '';
    var start = 0;
    var stop = N;
    while (b64.length > stop) {
        out += b64.slice(start, stop) + '\n';
        start += N;
        stop += N;
    }
    out += b64.slice(start, stop);
    return getPre(out);
}

function refreshSummary() {
    var table = document.getElementById('table_summary');

    var tr = document.createElement('tr');
    var field = getCell(START_TIME_UTC);
    var value = getCell(dateTimeUtc(params.start));
    field.id = START_FIELD_ID;
    value.id = START_VALUE_ID;
    tr.appendChild(field);
    tr.appendChild(value);
    table.appendChild(tr);

    tr = document.createElement('tr');
    field = getCell(STOP_TIME_UTC);
    value = getCell(dateTimeUtc(params.stop));
    field.id = STOP_FIELD_ID;
    value.id = STOP_VALUE_ID;
    tr.appendChild(field);
    tr.appendChild(value);
    table.appendChild(tr);

    if (params.hasOwnProperty('user_id')) {
        addSummaryRow(table, 'User ID', params.user_id);
    }
    if (params.hasOwnProperty('device_id')) {
        addSummaryRow(table, 'Device ID', params.device_id);
    }
    addSummaryRow(table, '# Events', params.event_count);
    if (params.event_count > events.length) {
        alert(params.event_count + ' events exist but we are only able to retrieve the first ' + events.length + ' events. Please zoom in to reduce the time window.')
    }
    if (params.hasOwnProperty('os_type')) {
        addSummaryRow(table, 'OS Type', params.os_type);
    }
    if (params.hasOwnProperty('os_version')) {
        addSummaryRow(table, 'OS Version', params.os_version);
    }
    if (params.hasOwnProperty('device_model')) {
        addSummaryRow(table, 'Device Model', params.device_model);
    }
    if (params.hasOwnProperty('build_version')) {
        addSummaryRow(table, 'Build Version', params.build_version);
    }
    if (params.hasOwnProperty('build_number')) {
        addSummaryRow(table, 'Build Number', params.build_number);
    }
}

function createTitleBar() {
    var tr = document.createElement('tr');

    var date = document.createElement('th');
    date.id = 'date_cell';
    date.className = 'cell';
    date.onclick = updateDate;
    date.title = SWITCH_TO_LOCAL;
    date.innerHTML = DATE_UTC;
    tr.appendChild(date);

    var time = document.createElement('th');
    time.id = 'time_cell';
    time.className = 'cell';
    time.onclick = updateDate;
    time.onclick = updateDate;
    time.title = SWITCH_TO_UTC;
    time.innerHTML = TIME_UTC;
    tr.appendChild(time);

    var eventType = document.createElement('th');
    eventType.className = 'cell';
    eventType.innerHTML = 'Event Class';
    tr.appendChild(eventType);

    if (show_user_id == 1) {
        var user_id = document.createElement('th');
        user_id.className = 'cell';
        user_id.innerHTML = 'User ID';
        tr.appendChild(user_id);
    }

    if (show_device_id == 1) {
        var device_id = document.createElement('th');
        device_id.className = 'cell';
        device_id.innerHTML = 'Device ID';
        tr.appendChild(device_id);
    }

    var module = document.createElement('th');
    module.className = 'cell';
    module.innerHTML = 'Module';
    tr.appendChild(module);

    var thread_id = document.createElement('th');
    thread_id.className = 'cell';
    thread_id.innerHTML = 'ThreadID';
    tr.appendChild(thread_id);

    var field = document.createElement('th');
    field.className = 'cell id_cell';
    field.innerHTML = 'Telemetry ID';
    tr.appendChild(field);

    var field = document.createElement('th');
    field.className = 'cell';
    field.innerHTML = 'Field';
    tr.appendChild(field);

    var value = document.createElement('th');
    value.className = 'cell';
    value.innerHTML = 'Value';
    tr.appendChild(value);

    return tr;
}

Object.prototype.notIn = function() {
    for(var i=0; i<arguments[0].length; i++)
       if(arguments[0][i] == this) return false;
    return true;
}

function refreshEvents() {
    var table = document.getElementById('table_events');
    if (0 < events.length) {
        table.appendChild(createTitleBar(table));
    }

    var last_timestamp = null;
    for (var i = 0; i < events.length; i++) {
        var event = events[i];
        var row;
        var current_timestamp = new Date(event.timestamp).getTime()/1000;
        if (event.module != "pinger-backend" && last_timestamp != null) {
            if (current_timestamp - last_timestamp > 20) {
                row = getDummyRow("WARN", "EXCESSIVE_TIME", "SECONDS SINCE LAST CLIENT EVENT: " + (current_timestamp - last_timestamp) + "<br/>");
                table.appendChild(row);
            }
        }
        last_timestamp = current_timestamp;

        switch (event.event_type) {
            case 'IMAP_REQUEST':
            case 'IMAP_RESPONSE': {
                row = getRowWithCommonFields(i, event, 1);
                var message = event.message.replace(/\n/g, "<br/>");
                var lines = event.message.split('\n')
                var previewLines = lines[0]
                if (lines.length >= 2) {
                    previewLines += "\n" + lines[1]
                }
                row.appendChild(getCell('message'));
                var valueCell = getCell(lines[0]);
                valueCell.id = i;
                valueCell.title = previewLines;
                valueCell.onclick = function() {
                    var event = events[this.id];
                    var message = event.message.replace(/\n/g, "<br/>");
                    var firstLine = event.message.split('\n')[0]
                    if (this.innerHTML == firstLine) {
                        this.innerHTML = message;
                    } else {
                        this.innerHTML = firstLine;
                    }
                    if (!isElementInViewport(this)) {
                        // if after collapsing the IMAP element is no longer visible,
                        // we should scroll it back in view.
                        this.scrollIntoView(true);
                        // TODO - need a better way to scroll. Right now, it always
                        // scroll to the left. It should track the amount of horizontal
                        // shift and undo that.
                        window.scrollBy(-10000, 0);
                    }
                }
                row.appendChild(valueCell);
                break;
            }
            case 'DEBUG':
            case 'INFO':
            case 'WARN':
            case 'ERROR': {
                if (event.module != 'pinger-backend') {
                    row = getRowWithCommonFields(i, event, 1);
                    var message = event.message.replace(/\n/g, "<br/>");
                    addFieldToRow(row, 'message', message);
                    table.appendChild(row)
                } else {
                    //row = getRowWithCommonFields(i, event, 1);
                    //addFieldToRow(row, 'message', event.message);
                    //table.appendChild(row);
                    var json = event;
                    var keys = Object.keys(json);
                    if (keys.length == 8) {
                        row = getRowWithCommonFields(i, event, 1);
                        addFieldToRow(row, 'message', event.message);
                        row.className += " pinger";
                        table.appendChild(row);
                    } else {
                        var isFirst = true;
                        for (var j = 0; j < keys.length; j++) {
                            var key = keys[j]
                            var dontShowKeys = ['pinger', 'user_id', 'id',
                            'module', 'event_type', 'timestamp', 'device_id'];
                            if (key.notIn(dontShowKeys)) {
                                if (isFirst) {
                                    row = getRowWithCommonFields(i, event, keys.length-dontShowKeys.length);
                                    row.className += " pinger";
                                    isFirst = false;
                                } else {
                                    table.appendChild(row);
                                    row = getRow(event);
                                    row.className += " pinger";
                                }
                                addFieldToRow(row, keys[j], json[keys[j]]);
                            } else {
                            }
                        }
                        table.appendChild(row);
                    }
                }
                break;
            }
            case 'DEVICEINFO': {
                try {
                    var json = event;
                    var keys = Object.keys(json);
                    if (keys.length == 0) {
                        row = getRowWithCommonFields(i, event, 1);
                        addFieldToRow(row, 'DEVICEINFO', event);
                    } else {
                        var isFirst = true;
                        for (var j = 0; j < keys.length; j++) {
                            if (isFirst) {
                                row = getRowWithCommonFields(i, event, keys.length);
                                isFirst = false;
                            } else {
                                table.appendChild(row);
                                row = getRow(event);
                            }
                            addFieldToRow(row, keys[j], json[keys[j]]);
                        }
                    }
                }
                catch (ex) {
                    console.log(ex)
                    row = getRowWithCommonFields(i, event, 1);
                    addFieldToRow(row, 'DEVICEINFO', event);
                }
                break;
            }
            case 'WBXML_REQUEST':
            case 'WBXML_RESPONSE': {
                row = getRowWithCommonFields(i, event, 1);
                row.appendChild(getCell('wbxml'));
                var valueCell = getCell(beautifyBase64(event.wbxml_base64));
                valueCell.id = i;
                valueCell.title = previewString(event.wbxml);
                valueCell.onclick = function() {
                    var event = events[this.id];
                    if (this.innerHTML == beautifyBase64(event.wbxml_base64)) {
                        this.innerHTML = getPre(event.wbxml);
                    } else {
                        this.innerHTML = beautifyBase64(event.wbxml_base64);
                    }
                    if (!isElementInViewport(this)) {
                        // if after collapsing the WBXML the element is no longer visible,
                        // we should scroll it back in view.
                        this.scrollIntoView(true);
                        // TODO - need a better way to scroll. Right now, it always
                        // scroll to the left. It should track the amount of horizontal
                        // shift and undo that.
                        window.scrollBy(-10000, 0);
                    }
                }
                row.appendChild(valueCell);
                break;
            }
            case 'UI': {
                var num_rows = 2;
                if (event.hasOwnProperty('ui_string')) {
                    num_rows += 1;
                }
                if (event.hasOwnProperty('ui_integer')) {
                    num_rows += 1;
                }

                row = getRowWithCommonFields(i, event, num_rows);
                addFieldToRow(row, 'ui_type', event.ui_type);
                table.appendChild(row);

                row = getRow(event);
                addFieldToRow(row, 'ui_object', event.ui_object);
                if (event.hasOwnProperty('ui_string')) {
                    table.appendChild(row);
                    row = getRow(event);
                    addFieldToRow(row, 'ui_string', event.ui_string);
                }
                if (event.hasOwnProperty('ui_integer')) {
                    table.appendChild(row);
                    row = getRow(event);
                    addFieldToRow(row, 'ui_integer', event.ui_integer);
                }
                break;
            }
            case 'COUNTER': {
                row = getRowWithCommonFields(i, event, 4);
                addFieldToRow(row, 'counter_name', event.counter_name)
                table.appendChild(row);

                row = getRow(event);
                addFieldToRow(row, 'count', event.count);
                table.appendChild(row);

                row = getRow(event);
                addFieldToRow(row, 'counter_start (UTC)', dateTimeUtc(event.counter_start));
                table.appendChild(row)

                row = getRow(event)
                addFieldToRow(row, 'counter_end (UTC)', dateTimeUtc(event.counter_end));
                break;
            }
            case 'SAMPLES': {
                row = getRowWithCommonFields(i, event, 2);
                addFieldToRow(row, 'sample_name', event.sample_name)
                table.appendChild(row);
                if (event.hasOwnProperty('sample_value')) {
                    row = getRow(event);
                    addFieldToRow(row, 'sample_value', event.sample_value);
                    table.appendChild(row);
                }
                if (event.hasOwnProperty('sample_int')) {
                    row = getRow(event);
                    addFieldToRow(row, 'sample_int', event.sample_int);
                    table.appendChild(row);
                }
                else if (event.hasOwnProperty('sample_float')) {
                    row = getRow(event);
                    addFieldToRow(row, 'sample_float', event.sample_float);
                    table.appendChild(row);
                }
                else if (event.hasOwnProperty('sample_string')) {
                    row = getRow(event);
                    addFieldToRow(row, 'sample_string', event.sample_string);
                    table.appendChild(row);
                }
                break;
            }
            case 'TIME_SERIES': {
                row = getRowWithCommonFields(i, event, 3);
                addFieldToRow(row, 'time_series_name', event.time_series_name)
                table.appendChild(row);
                row = getRow(event);
                addFieldToRow(row, 'time_series_timestamp', event.time_series_timestamp)
                table.appendChild(row);
                if (event.hasOwnProperty('time_series_value')) {
                    row = getRow(event);
                    addFieldToRow(row, 'time_series_value', event.time_series_value);
                    table.appendChild(row);
                }
                if (event.hasOwnProperty('time_series_int')) {
                    row = getRow(event);
                    addFieldToRow(row, 'time_series_int', event.time_series_int);
                    table.appendChild(row);
                }
                else if (event.hasOwnProperty('time_series_float')) {
                    row = getRow(event);
                    addFieldToRow(row, 'time_series_float', event.time_series_float);
                    table.appendChild(row);
                }
                else if (event.hasOwnProperty('time_series_string')) {
                    row = getRow(event);
                    addFieldToRow(row, 'time_series_string', event.time_series_string);
                    table.appendChild(row);
                }
                break;
            }
            case 'STATISTICS2': {
                row = getRowWithCommonFields(i, event, 8);
                addFieldToRow(row, 'stat2_name', event.stat2_name);
                table.appendChild(row);

                row = getRow(event);
                addFieldToRow(row, 'count', event.count);
                table.appendChild(row);

                row = getRow(event);
                addFieldToRow(row, 'min', event.min);
                table.appendChild(row);

                row = getRow(event);
                addFieldToRow(row, 'max', event.max);
                table.appendChild(row);

                var average = 0.0;
                var moment2 = 0.0;
                if (0 < event.count) {
                    average = event.sum / event.count;
                    moment2 = event.sum2 / event.count;
                }
                var variance = moment2 - (average * average);

                row = getRow(event);
                addFieldToRow(row,'average', average);
                table.appendChild(row);

                if (0 <= variance) {
                    stddev = Math.sqrt(variance);
                    row = getRow(event);
                    addFieldToRow(row, 'stddev', stddev);
                    table.appendChild(row);
                }

                row = getRow(event);
                addFieldToRow(row, 'sum', event.sum);
                table.appendChild(row);

                row = getRow(event);
                addFieldToRow(row, 'sum2', event.sum2);
                break;
            }
            case 'SUPPORT': {
                try {
                    var json = JSON.parse(event.support);
                    var keys = Object.keys(json);
                    if (keys.length == 0) {
                        row = getRowWithCommonFields(i, event, 1);
                        addFieldToRow(row, 'support', event.support);
                    } else {
                        var isFirst = true;
                        for (var j = 0; j < keys.length; j++) {
                            if (isFirst) {
                                row = getRowWithCommonFields(i, event, keys.length);
                                isFirst = false;
                            } else {
                                table.appendChild(row);
                                row = getRow(event);
                            }
                            addFieldToRow(row, keys[j], json[keys[j]]);
                        }
                    }
                }
                catch (ex) {
                    row = getRowWithCommonFields(i, event, 1);
                    addFieldToRow(row, 'support', event.support);
                }
                break;
            }
            default: {
                row = getRowWithCommonFields(i, event, 1);
                break;
            }
        }
        table.appendChild(row);
    }
}

function refresh() {
    refreshSummary();
    refreshEvents();
}

function updateDate() {
    isUtc = !isUtc;

    // Update event table header
    var dateCell = document.getElementById('date_cell');
    var timeCell = document.getElementById('time_cell');
    if (isUtc) {
        dateCell.innerHTML = DATE_UTC;
        timeCell.innerHTML = TIME_UTC;
        dateCell.title = SWITCH_TO_LOCAL;
        timeCell.title = SWITCH_TO_LOCAL;
    } else {
        dateCell.innerHTML = DATE_LOCAL;
        timeCell.innerHTML = TIME_LOCAL;
        dateCell.title = SWITCH_TO_UTC;
        timeCell.title = SWITCH_TO_UTC;
    }

    // Update event table timestamp
    for (var i = 0; i < events.length; i++) {
        var dateCell = document.getElementById('date_' + i);
        var timeCell = document.getElementById('time_' + i);
        date = new Date(events[i].timestamp);
        if (dateCell) {
            dateCell.innerHTML = isUtc ? dateUtc(date) : dateLocal(date);
        }
        if (timeCell) {
            timeCell.innerHTML = isUtc ? timeUtc(date) : timeLocal(date);
        }
    }

    // Update summary table
    var startField = document.getElementById(START_FIELD_ID);
    var startValue = document.getElementById(START_VALUE_ID);
    var stopField = document.getElementById(STOP_FIELD_ID);
    var stopValue = document.getElementById(STOP_VALUE_ID);

    if (isUtc) {
        startField.innerHTML = START_TIME_UTC;
        stopField.innerHTML = STOP_TIME_UTC;
        startValue.innerHTML = dateTimeUtc(params.start);
        stopValue.innerHTML = dateTimeUtc(params.stop);
    } else {
        startField.innerHTML = START_TIME_LOCAL;
        stopField.innerHTML = STOP_TIME_LOCAL;
        startValue.innerHTML = dateTimeLocal(params.start);
        stopValue.innerHTML = dateTimeLocal(params.stop);
    }
}

// encode(decode) html text into html entity
var decodeHtmlEntity = function(str) {
  var result = str.replace(/&#(\d+);/g, function(match, dec) {
    return String.fromCharCode(dec);
  });
  return str.replace(/&(\w+);/g, function (match, dec) {
    switch (dec) {
     case "lt": return "<";
     case "gt": return ">";
     case "amp": return "&";
     case "quot": return '"';
     default: return dec;
    }
  });
};

if (typeof String.prototype.startsWith != 'function') {
  String.prototype.startsWith = function (str){
    return this.slice(0, str.length) == str;
  };
}

function dumpEventsString() {
   var data = "";
    for (var i = 0; i < events.length; i++) {
       var line = "";
       line += events[i].timestamp + "\t";
       if (typeof events[i].thread_id == "undefined") {
         line += "0:";
       } else {
         line += events[i].thread_id + ":";
       }
       line += events[i].event_type + ":";
       line += events[i].module + "\t";
       switch (events[i].event_type) {
        case 'UI': {
            var ui = "";
            ui += 'ui_type='+events[i].ui_type + " ";
            ui += 'ui_object='+events[i].ui_object + " ";
            if (events[i].hasOwnProperty('ui_string')) {
                ui += "ui_string="+events[i].ui_string + " ";
            }
            if (events[i].hasOwnProperty('ui_integer')) {
                ui += "ui_integer="+events[i].ui_integer + " ";
            }
            line += ui;
            break;
        }

        case 'SUPPORT':
            line += events[i].support;
            break;

        case 'WBXML_REQUEST':
        case 'WBXML_RESPONSE':
            line += events[i].wbxml;
            break;

        case 'DEVICEINFO': {
            var keys = Object.keys(events[i]);
            if (keys.length > 0) {
                for (var j = 0; j < keys.length; j++) {
                    line += keys[j] + "=" + events[i][keys[j]] + " ";
                }
            }
            break;
        }

        case 'COUNTER': {
            var keys = Object.keys(events[i]);
            for (var j = 0 ; j < keys.length; j++) {
                if (keys[j].startsWith('counter_')) {
                    line += keys[j] + "=" + events[i][keys[j]] + " ";
                }
            }
            line += "count=" + events[i].count;
            break;
        }

        case 'SAMPLES': {
            var keys = Object.keys(events[i]);
            for (var j = 0 ; j < keys.length; j++) {
                if (keys[j].startsWith('sample_')) {
                    line += keys[j] + "=" + events[i][keys[j]] + " ";
                }
            }
            break;
        }

        case 'TIME_SERIES': {
            var keys = Object.keys(events[i]);
            for (var j = 0 ; j < keys.length; j++) {
                if (keys[j].startsWith('time_series_')) {
                    line += keys[j] + "=" + events[i][keys[j]] + " ";
                }
            }
            break;
        }

        case 'STATISTICS2':
            line += "stat2_name=" + events[i].stat2_name + " ";
            line += "count=" + events[i].count + " ";
            line += "min=" + events[i].min + " ";
            line += "max=" + events[i].max + " ";
            line += "sum=" + events[i].sum + " ";
            line += "sum2=" + events[i].sum2 + " ";
            break;

        default:
           if (typeof events[i].message == "undefined") {
             console.log(events[i]);
           }
           line += events[i].message;
       }
       data += line.replace(/\n/g, "  ") + "\n";
    }
    return decodeHtmlEntity(data);
}

function download(filename, text) {
    var pom = document.createElement('a');
    pom.setAttribute('href', 'data:text/plain;charset=utf-8,' + encodeURIComponent(text));
    pom.setAttribute('download', filename);

    if (document.createEvent) {
        var event = document.createEvent('MouseEvents');
        event.initEvent('click', true, true);
        pom.dispatchEvent(event);
    }
    else {
        pom.click();
    }
}

function downloadWithLocation() {
    window.location='data:application/octet-stream;base64,' + btoa(dumpData());
}

function downloadWithHtml5() {
    download("telemetry-data.txt", dumpEventsString());
}
