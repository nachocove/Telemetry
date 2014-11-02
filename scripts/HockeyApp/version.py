from error import HockeyAppError


class Version:
    def __init__(self, app_obj, version_id=None, version=None, short_version=None):
        self.app_obj = app_obj
        self.version_id = version_id
        if self.version_id is not None:
            self.base_url = self.app_obj.base_url + '/app_versions/' + str(self.version_id)
        else:
            self.base_url = None
        self.version = version
        self.short_version = short_version

    def __eq__(self, other):
        return (self.version_id == other.version_id and
                self.version == other.version and
                self.short_version == other.short_version and
                self.base_url == other.base_url)

    def desc(self):
        return '<HockeyApp.Version: %s %s %s [%s: %s]>' % (self.short_version, self.version, self.version_id,
                                                           self.app_obj.title, self.app_obj.app_id)

    def __repr__(self):
        return self.desc()

    def __str__(self):
        return self.desc()

    def update(self, zipped_dsym_file, note=None):
        form_data = dict()
        form_data['dsym'] = '@' + zipped_dsym_file
        if note is not None:
            form_data['note'] = note
        response = self.app_obj.hockeyapp_obj.command(self.base_url, form_data).put().run()
        return response

    def create(self):
        form_data = dict()
        if self.version is None:
            raise ValueError('version is not initialized')
        else:
            form_data['bundle_version'] = self.version
        if self.short_version is None:
            raise ValueError('short_version is not initialized')
        else:
            form_data['bundle_short_version'] = self.short_version

        response = self.app_obj.hockeyapp_obj.command(self.app_obj.base_url + '/app_versions/new',
                                                      form_data).post().run()
        if 'errors' in response:
            raise HockeyAppError(response)
        if 'id' not in response:
            raise HockeyAppError('no id in response')
        self.version_id = response['id']
        self.base_url = self.app_obj.base_url + '/app_versions/' + str(self.version_id)
        return self

    def read(self):
        version_list = self.app_obj.versions()
        for version in version_list:
            if version.version_id == self.version_id:
                self.version = version.version
                self.short_version = version.short_version
                break
        else:
            raise ValueError('Unknown version id %s for app id %s' % (self.version_id, self.app_obj.app_id))
        return self

    def delete(self):
        self.app_obj.hockeyapp_obj.command(self.base_url, {'strategy': 'purge'}).delete().run()
