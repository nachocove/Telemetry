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