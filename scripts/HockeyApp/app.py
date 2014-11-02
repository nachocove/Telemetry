from error import HockeyAppError
from version import Version
from crash import CrashGroup


class App:
    RELEASE_TYPES = {'beta': 0,
                     'live': 1,
                     'alpha': 2}

    PLATFORMS = ('iOS', 'Android', 'Mac OS', 'Windows Phone')

    @staticmethod
    def check_release_type(release_type):
        if release_type is not None and release_type not in App.RELEASE_TYPES:
            raise ValueError('Invalid release type. Choices are: ' + ' '.join(App.RELEASE_TYPES.keys()))

    @staticmethod
    def check_platform(platform):
        if platform is not None and platform not in App.PLATFORMS:
            raise ValueError('Invalid platform type. Choices are: ' + ' '.join(App.PLATFORMS))

    @staticmethod
    def release_type_from_value(release_type_value):
        for (type_, value) in App.RELEASE_TYPES.items():
            if value != release_type_value:
                continue
            return type_
        else:
            raise ValueError('unknown release type value %s' % str(release_type_value))

    def __init__(self, hockeyapp_obj, app_id=None, id=None,
                 title=None, bundle_id=None, platform=None, release_type=None):
        self.hockeyapp_obj = hockeyapp_obj
        self.app_id = app_id
        self.id = id
        if self.app_id is not None:
            self.base_url = self.hockeyapp_obj.base_url + '/apps/' + self.app_id
        else:
            self.base_url = None
        self.title = title
        self.bundle_id = bundle_id
        App.check_platform(platform)
        self.platform = platform
        App.check_release_type(release_type)
        self.release_type = release_type

    def __eq__(self, other):
        return (self.app_id == other.app_id and
                self.base_url == other.base_url and
                self.title == other.title and
                self.bundle_id == other.bundle_id and
                self.platform == other.platform and
                self.release_type == other.release_type)

    def desc(self):
        return '<HockeyApp.App: %s %s [%s: %s]>' % (self.title, self.app_id, self.platform, self.bundle_id)

    def __repr__(self):
        return self.desc()

    def __str__(self):
        return self.desc()

    def versions(self):
        """
        List all versions of this app. Return a list of Version objects.
        """
        response = self.hockeyapp_obj.command(self.base_url + '/app_versions').get().run()
        if response['status'] != 'success':
            raise ValueError('Server returns failures (status=%s)' % response['status'])
        version_list = []
        for version_data in response['app_versions']:
            version = Version(app_obj=self,
                              version_id=version_data['id'],
                              version=version_data['version'],
                              short_version=version_data['shortversion'])
            version_list.append(version)
        return version_list

    def version(self, version_id):
        """
        Return a Version object that represents a single version of this app.
        """
        return Version(app_obj=self,
                       version_id=version_id).read()

    def create(self):
        form_data = dict()
        if self.title is None:
            raise ValueError('title is not initialized')
        else:
            form_data['title'] = self.title
        if self.bundle_id is None:
            raise ValueError('bundle_id is not initialized')
        else:
            form_data['bundle_identifier'] = self.bundle_id
        App.check_platform(self.platform)
        if self.platform is not None:
            form_data['platform'] = self.platform
        App.check_release_type(self.release_type)
        if self.release_type is not None:
            form_data['release_type'] = App.RELEASE_TYPES[self.release_type]

        response = self.hockeyapp_obj.command(self.hockeyapp_obj.base_url + '/apps/new', form_data).post().run()
        if 'errors' in response:
            raise HockeyAppError(response)
        if 'public_identifier' not in response:
            raise HockeyAppError('no public_identifier in response')
        self.app_id = str(response['public_identifier'])
        self.base_url = self.hockeyapp_obj.base_url + '/apps/' + self.app_id
        return self

    def read(self):
        app_list = self.hockeyapp_obj.apps()
        for app in app_list:
            if app.app_id != self.app_id:
                continue
            self.id = app.id
            self.title = app.title
            self.bundle_id = app.bundle_id
            self.platform = app.platform
            self.release_type = app.release_type
            break
        else:
            raise ValueError('Unknown app id %s' % self.app_id)
        return self

    def delete(self):
        self.hockeyapp_obj.command(self.base_url).delete().run()

    def find_version(self, version, short_version):
        for version_obj in self.versions():
            if version_obj.short_version == short_version and version_obj.version == version:
                return version_obj
        return None

    def crash_groups(self):
        """
        List all crash groups of this app. Return a list of CrashGroup objects.
        """
        crash_group_list = list()
        page = 1
        total_pages = 1
        while page <= total_pages:
            response = self.hockeyapp_obj.command(self.base_url + '/crash_reasons', {'page': page}).get().run()
            if response['status'] != 'success':
                raise ValueError('Server returns failures (status=%s)' % response['status'])
            total_pages = response['total_pages']
            for crash_group_data in response['crash_reasons']:
                crash_group = CrashGroup(app_obj=self,
                                         crash_group_id=int(crash_group_data['id']),
                                         version_id=crash_group_data['app_version_id'],
                                         reason=crash_group_data['reason'],
                                         last_crash_at=crash_group_data['last_crash_at'],
                                         status=crash_group_data['status'],
                                         number_of_crashes=int(crash_group_data['number_of_crashes']))
                crash_group_list.append(crash_group)
            page += 1
        return crash_group_list