import re


class CrashGroup:
    def __init__(self, app_obj, crash_group_id=None, version_id=None, reason=None, last_crash_at=None,
                 status=None, number_of_crashes=None):
        self.app_obj = app_obj
        self.crash_group_id = crash_group_id
        self.version_id = version_id
        if self.crash_group_id is not None:
            self.base_url = self.app_obj.base_url + '/crash_reasons/' + str(self.crash_group_id)
        else:
            self.base_url = None
        self.reason = reason
        self.last_crash_at = last_crash_at
        self.status = status
        self.number_of_crashes = number_of_crashes

    def desc(self):
        return '<HockeyApp.CrashGroup: %s %s>' % (self.crash_group_id, self.version_id)

    def __repr__(self):
        return self.desc()

    def __str__(self):
        return self.desc()

    def crashes(self):
        """
        List all crashes of this group.
        """
        response = self.app_obj.hockeyapp_obj.command(self.base_url).get().run()
        if 'status' not in response:
            raise ValueError('Server returns malformed response (response=%s)' % response)
        if response['status'] != 'success':
            raise ValueError('Server returns failures (status=%s)' % response['status'])

        crash_list = list()
        for crash_data in response['crashes']:
            crash = Crash(crash_group_obj=self,
                          crash_id=int(crash_data['id']),
                          created_at=crash_data['created_at'],
                          has_log=crash_data['has_log'],
                          has_description=crash_data['has_description'])
            crash_list.append(crash)
        return crash_list


class Crash:
    def __init__(self, crash_group_obj, crash_id=None, created_at=None, has_log=False, has_description=False):
        self.crash_group_obj = crash_group_obj
        self.crash_id = crash_id
        if self.crash_id is not None:
            self.base_url = self.crash_group_obj.app_obj.base_url + '/crashes/' + str(self.crash_id)
        else:
            self.base_url = None
        self.created_at = str(created_at)
        self.has_log = has_log
        self.has_description = has_description

    def desc(self):
        return '<HockeyApp.Crash: %s>' % self.crash_id

    def __repr__(self):
        return self.desc()

    def __str__(self):
        return self.desc()

    def read_log(self):
        if not self.has_log:
            return None
        ha_obj = self.crash_group_obj.app_obj.hockeyapp_obj
        response = ha_obj.command(self.base_url, {'format': 'log'}).get().follow_redirect().run(raw=True)
        return response

    def read_description(self):
        if not self.has_description:
            return None
        ha_obj = self.crash_group_obj.app_obj.hockeyapp_obj
        response = ha_obj.command(self.base_url, {'format': 'text'}).get().follow_redirect().run(raw=True)
        return response


class CrashDescription:
    """
    A custom parser for handling our own version of crash log description.
    """
    regex = {'version': re.compile('Version: (?P<version>\S+)'),
             'build_number': re.compile('Build Number: (?P<build_number>[0-9]+)'),
             'launch_time': re.compile('Launch Time: (?P<launch_time>.+)'),
             'device_id': re.compile('Device ID: (?P<device_id>\S+)'),
             'build_time': re.compile('Build Time: (?P<build_time>.+)'),
             'build_user': re.compile('Build User: (?P<build_user>.+)'),
             'source': re.compile('Source: (?P<source>.+)')}

    def __init__(self, desc):
        self.desc = desc
        self.version = None
        self.build_number = None
        self.launch_time = None
        self.device_id = None
        self.build_time = None
        self.build_user = None
        self.source = None

        # Parse the string
        for line in self.desc.split('\n'):
            for (key, regex) in self.regex.items():
                match = self.regex[key].match(line)
                if match:
                    setattr(self, key, match.group(key))
                    break
            # There was a bug in AppDelegate.cs that put device id and build
            # time in the same line. This exception logic handles this bug
            match = re.match('^Device ID: (?P<device_id>\S+)Build Time: (?P<build_time>.+)$', line)
            if match:
                self.device_id = match.group('device_id')
                self.build_time = match.group('build_time')

    def __str__(self):
        s = 'Version:      %s\n' % self.version
        s += 'Build number: %s\n' % self.build_number
        s += 'Launch time:  %s\n' % self.launch_time
        s += 'Device ID:    %s\n' % self.device_id
        s += 'Build time:   %s\n' % self.build_time
        s += 'Build user:   %s\n' % self.build_user
        s += 'Source:       %s\n' % self.source
        return s
