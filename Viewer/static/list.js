function getRow (event) {
    var tr = document.createElement('tr');

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
    tr.className = event_type;
    return tr;
}

function getCell (html, rowSpan) {
    if (typeof rowSpan == 'undefined') {
        rowSpan = 1; // default is 1 row
    }
    var td = document.createElement('td');
    td.className = 'cell';
    td.innerHTML = html;
    td.rowSpan = rowSpan;
    return td;
}

function getRowWithCommonFields (event, num_rows) {
    var tr = getRow(event);
    tr.appendChild(getCell(event.date, num_rows));
    tr.appendChild(getCell(event.time, num_rows));
    tr.appendChild(getCell(event.event_type.replace('_', ' '), num_rows));
    return tr;
}

function getPre (html) {
    return '<pre>' + html + '</pre>';
}

function addFieldToRow (row, field, value) {
    row.appendChild(getCell(field));
    valueCell = getCell(value);
    row.appendChild(valueCell);
    return valueCell;
}

function refresh() {
    var table = document.getElementById('table_events');
    for (var i = 0; i < events.length; i++) {
        var event = events[i];
        var row;
        switch (event.event_type) {
            case 'DEBUG':
            case 'INFO':
            case 'WARN':
            case 'ERROR': {
                row = getRowWithCommonFields(event, 1);
                addFieldToRow(row, 'message', event.message);
                break;
            }
            case 'WBXML_REQUEST':
            case 'WBXML_RESPONSE': {
                row = getRowWithCommonFields(event, 1);
                row.appendChild(getCell('wbxml'));
                valueCell = getCell(event.wbxml_base64);
                valueCell.id = i;
                valueCell.onclick = function() {
                    var event = events[this.id];
                    if (this.innerHTML == event.wbxml_base64) {
                        this.innerHTML = getPre(event.wbxml);
                    } else {
                        this.innerHTML = event.wbxml_base64;
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

                row = getRowWithCommonFields(event, num_rows);
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
            default: {
                row = getRowWithCommonFields(event, 1);
                break;
            }
        }
        table.appendChild(row);
    }
}