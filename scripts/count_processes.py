# Copyright 2014, NachoCove, Inc


# 2015-08-10T11:16:59.365Z        34:INFO:SYS     NcTask Start13 started, 27 running
# 2015-08-10T11:16:59.575Z        34:INFO:SYS     NcTask Start13 completed.
# 2015-08-10T11:16:59.578Z        34:INFO:SYS     NcTask ImapFetchBodyCommand41 started, 28 running
# 2015-08-10T11:16:59.868Z        35:INFO:SYS     NcTask Start14 started, 28 running
# 2015-08-10T11:16:59.919Z        35:INFO:SYS     NcTask Start14 completed.
import re
import sys

started_task_details = {}
task_details = {}

for line in sys.stdin:
    line = line.rstrip()
    (linedate, meta, message) = line.split('\t')
    match = re.match(r'^NcTask (?P<name>[a-zA-Z:]+)(?P<num>\d+) (?P<action>(started|completed)).*$', message)
    if not match:
        print "WARN: No match: %s" % line
        continue

    job = "%s%s" % (match.group("name"), match.group("num"))
    started_task_details.setdefault(match.group("name"), {})
    if match.group("action") == "started":
        if match.group("num") in started_task_details[match.group("name")]:
            print "ERROR: Job %s already started!" % job
        else:
            started_task_details[match.group("name")][match.group("num")] = (linedate, line)
    elif match.group("action") == "completed":
        if match.group("num") in started_task_details[match.group("name")]:
            del started_task_details[match.group("name")][match.group("num")]

bydate = {}
for name in started_task_details:
    if started_task_details[name]:
        for num in started_task_details[name]:
            mydate = started_task_details[name][num][0]
            if mydate in bydate:
                print "Duplicate timestamp %s" % mydate
            bydate[mydate] = started_task_details[name][num][1]

for l in sorted(bydate):
    print bydate[l]