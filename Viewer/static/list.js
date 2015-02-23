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
    var out = s.toString();
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
    // Set up the style for the row
    var event_type;
    switch (event.event_type) {
        case 'WBXML_REQUEST':
        case 'WBXML_RESPONSE':
            event_type = 'wbxml';
            break;
        default:
            event_type = event.event_type.toLowerCase();
            break;
    }
    return $('<tr></tr>').addClass(event_type).attr('id', event.id);
}

function getCell(html, rowSpan) {
    if (typeof rowSpan == 'undefined') {
        rowSpan = 1; // default is 1 row
    }
    return $('<td></td>').html(html).addClass('cell').attr('rowSpan', rowSpan);
}

function getRowWithCommonFields (id, event, num_rows) {
    var tr = getRow(event);
    var iso = new Date(event.timestamp);

    var date = getCell(dateUtc(iso), num_rows);
    date.attr('id', 'date_' + id);
    tr.append(date);

    var time = getCell(timeUtc(iso), num_rows);
    time.attr('id', 'time_' + id);
    tr.append(time);

    tr.append(getCell(event.event_type.replace('_', ' '), num_rows));

    var id_cell = getCell(event.id, num_rows);
    id_cell.addClass("id_cell");
    tr.append(id_cell);
    return tr;
}

function addFieldToRow(row, field, value) {
    row.append(getCell(field));
    var valueCell = getCell(value);
    row.append(valueCell);
    return valueCell;
}

function addSummaryRow(table, field, value) {
    var tr = $('<tr></tr>');
    tr.append(getCell(field));
    tr.append(getCell(value));
    table.append(tr);
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
    return htmlUnescape(s.slice(0, N)) + '...';
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
    return $('<pre></pre>').addClass('wbxmlb64').html(out);
}

function refreshSummary() {
    var table = $('#table_summary');
    var tr = $('<tr></tr>');

    var field = getCell(START_TIME_UTC);
    field.attr('id', START_FIELD_ID);
    tr.append(field);

    var value = getCell(dateTimeUtc(params.start));
    value.attr('id', START_VALUE_ID);
    tr.append(value);

    table.append(tr);

    tr = $('<tr></tr>');
    field = $('<td></td>').text(STOP_TIME_UTC);
    field = getCell(STOP_TIME_UTC);
    field.attr('id', STOP_FIELD_ID);
    tr.append(field);

    value = getCell(dateTimeUtc(params.stop));
    value.attr('id', STOP_VALUE_ID);
    tr.append(value);

    table.append(tr);

    addSummaryRow(table, 'Client', params.client);
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
    var tr = $('<tr></tr>');
    tr.append($('<th></th>').attr('id', 'date_cell').attr('title', SWITCH_TO_LOCAL).addClass('cell').click(updateDate).html(DATE_UTC));
    tr.append($('<th></th>').attr('id', 'time_cell').attr('title', SWITCH_TO_UTC).addClass('cell').click(updateDate).html(TIME_UTC));
    tr.append($('<th></th>').addClass('cell').html('Event Type'));
    tr.append($('<th></th>').addClass('cell').addClass('id_cell').html('Telemetry ID'));
    tr.append($('<th></th>').addClass('cell').html('Field'));
    tr.append($('<th></th>').addClass('cell').html('Value').attr('colSpan', 2));
    return tr;
}

function refreshEvents() {
    var table = $('#table_events');
    if (0 < events.length) {
        table.append(createTitleBar(table));
    }
    for (var i = 0; i < events.length; i++) {
        var event = events[i];
        var row;
        switch (event.event_type) {
            case 'DEBUG':
            case 'INFO':
            case 'WARN':
            case 'ERROR': {
                row = getRowWithCommonFields(i, event, 2);
                addFieldToRow(row, 'thread_id', event.thread_id);
                table.append(row)
                row = getRow(event);
                addFieldToRow(row, 'message', event.message);
                break;
            }
            case 'WBXML_REQUEST':
            case 'WBXML_RESPONSE': {
                row = getRowWithCommonFields(i, event, 1);
                row.append(getCell('wbxml'));
                valueCell = getCell(beautifyBase64(event.wbxml_base64));
                valueCell.attr('id', i);
                valueCell.attr('title', previewString(event.wbxml));
                valueCell.click(function() {
                    var event = events[this.id];
                    if (this.html() == beautifyBase64(event.wbxml_base64)) {
                        this.html($('<pre></pre>').addClass('wbxml').html(event.wbxml));
                    } else {
                        this.html(beautifyBase64(event.wbxml_base64));
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
                });
                row.append(valueCell);
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
                table.append(row);

                row = getRow(event);
                addFieldToRow(row, 'ui_object', event.ui_object);
                if (event.hasOwnProperty('ui_string')) {
                    table.append(row);
                    row = getRow(event);
                    addFieldToRow(row, 'ui_string', event.ui_string);
                }
                if (event.hasOwnProperty('ui_integer')) {
                    table.append(row);
                    row = getRow(event);
                    addFieldToRow(row, 'ui_integer', event.ui_integer);
                }
                break;
            }
            case 'COUNTER': {
                row = getRowWithCommonFields(i, event, 4);
                addFieldToRow(row, 'counter_name', event.counter_name)
                table.append(row);

                row = getRow(event);
                addFieldToRow(row, 'count', event.count);
                table.append(row);

                row = getRow(event);
                addFieldToRow(row, 'counter_start (UTC)', dateTimeUtc(event.counter_start));
                table.append(row)

                row = getRow(event)
                addFieldToRow(row, 'counter_end (UTC)', dateTimeUtc(event.counter_end));
                break;
            }
            case 'CAPTURE': {
                row = getRowWithCommonFields(i, event, 8);
                addFieldToRow(row, 'capture_name', event.capture_name);
                table.append(row);

                row = getRow(event);
                addFieldToRow(row, 'count', event.count);
                table.append(row);

                row = getRow(event);
                addFieldToRow(row, 'min', event.min);
                table.append(row);

                row = getRow(event);
                addFieldToRow(row, 'max', event.max);
                table.append(row);

                var average = 0.0;
                var moment2 = 0.0;
                if (0 < event.count) {
                    average = event.sum / event.count;
                    moment2 = event.sum2 / event.count;
                }
                var variance = moment2 - (average * average);

                row = getRow(event);
                addFieldToRow(row,'average', average);
                table.append(row);

                if (0 <= variance) {
                    stddev = Math.sqrt(variance);
                    row = getRow(event);
                    addFieldToRow(row, 'stddev', stddev);
                    table.append(row);
                }

                row = getRow(event);
                addFieldToRow(row, 'sum', event.sum);
                table.append(row);

                row = getRow(event);
                addFieldToRow(row, 'sum2', event.sum2);
                break;
            }
            case 'SUPPORT': {
                try {
                    var json = JSON.parse(event.support);
                    var keys = Object.keys(json);
                    var isFirst = true;
                    for (var j = 0; j < keys.length; j++) {
                        if (isFirst) {
                            row = getRowWithCommonFields(i, event, keys.length);
                            isFirst = false;
                        } else {
                            table.append(row);
                            row = getRow(event);
                        }
                        addFieldToRow(row, keys[j], json[keys[j]]);
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
        table.append(row);
    }
}

function refresh() {
    refreshSummary();
    refreshEvents();
}

function hide_event_row_count() {
    $('#table_event_shown').hide()
}
function display_event_row_count() {
    hide_event_row_count()
    $('#table_event_count').text($('#table_events tr:visible').length)
    $('#table_event_shown').show()
}

function updateDate() {
    isUtc = !isUtc;

    // Update event table header
    var dateCell = $('#date_cell');
    var timeCell = $('#time_cell');
    if (isUtc) {
        dateCell.html(DATE_UTC);
        timeCell.html(TIME_UTC);
        dateCell.attr('title', SWITCH_TO_LOCAL);
        timeCell.attr('title', SWITCH_TO_LOCAL);
    } else {
        dateCell.html(DATE_LOCAL);
        timeCell.html(TIME_LOCAL);
        dateCell.attr('title', SWITCH_TO_UTC);
        timeCell.attr('title', SWITCH_TO_UTC);
    }

    // Update event table timestamp
    for (var i = 0; i < events.length; i++) {
        var dateCell = $('#date_' + i);
        var timeCell = $('#time_' + i);
        date = new Date(events[i].timestamp);
        dateCell.html(isUtc ? dateUtc(date) : dateLocal(date));
        timeCell.html(isUtc ? timeUtc(date) : timeLocal(date));
    }

    // Update summary table
    var startField = $('#'+START_FIELD_ID);
    var startValue = $('#'+START_VALUE_ID);
    var stopField = $('#'+STOP_FIELD_ID);
    var stopValue = $('#'+STOP_VALUE_ID);

    if (isUtc) {
        startField.html(START_TIME_UTC);
        stopField.html(STOP_TIME_UTC);
        startValue.html(dateTimeUtc(params.start));
        stopValue.html(dateTimeUtc(params.stop));
    } else {
        startField.html(START_TIME_LOCAL);
        stopField.html(STOP_TIME_LOCAL);
        startValue.html(dateTimeLocal(params.start));
        stopValue.html(dateTimeLocal(params.stop));
    }
}
