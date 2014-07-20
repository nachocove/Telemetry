import subprocess
import json
import time
from app import App


class CurlCommand:
    """
    Simple wrapper around curl command. Instead of writing our own
    Python-based URL driver, we'll just use curl since it does the same
    thing with much less work. curl supports multipart/form-data better
    than urllib.
    """
    def __init__(self, verbose=False):
        self.verbose = verbose
        if self.verbose:
            self.params = ['-v']
        else:
            self.params = ['--silent']
        self.url_ = None

    def form_data(self, field, value):
        """
        Wrap -F
        """
        self.params.extend(['-F', '%s=%s' % (field, value)])
        return self

    def header(self, field, value):
        """
        Wrap -H
        """
        self.params.extend(['-H', '%s: %s' % (field, value)])
        return self

    def follow_redirect(self):
        self.params.append('-L')
        return self

    def url(self, url_):
        self.url_ = url_
        return self

    def put(self):
        self.params.extend(['-X', 'PUT'])
        return self

    def get(self):
        self.params.extend(['-X', 'GET'])
        return self

    def post(self):
        self.params.extend(['-X', 'POST'])
        return self

    def delete(self):
        self.params.extend(['-X', 'DELETE'])
        return self

    def _command(self):
        return ['curl'] + self.params + [self.url_]

    def __str__(self):
        def quotify(s):
            if ' ' in s:
                return '"%s"' % s
            return s
        return ' '.join([quotify(x) for x in self._command()])

    def run(self, raw=False):
        time.sleep(0.5)
        if self.url is None:
            raise ValueError('URL is not set')

        output = subprocess.check_output(self._command())
        if raw:
            # For attachment download
            return output
        try:
            return json.loads(output)
        except ValueError:
            print 'Unexpected response from server\n', output
            return {}


class HockeyApp:
    def __init__(self, api_token):
        self.api_token = api_token
        self.base_url = 'https://rink.hockeyapp.net/api/2'

    def base_command(self):
        return CurlCommand().header('X-HockeyAppToken', self.api_token)

    def command(self, url_, form_data=None):
        cmd = self.base_command()
        if isinstance(form_data, dict):
            for (field, value) in form_data.items():
                cmd.form_data(field, value)
        cmd.url(url_)
        return cmd

    def url(self, path=None):
        if path is None:
            return self.base_url
        return self.base_url + path

    def apps(self):
        app_list = []
        response = self.command(self.base_url + '/apps').get().run()
        assert isinstance(response, dict)
        if response['status'] != 'success':
            raise ValueError('Server returns failures (status=%s)' % response['status'])
        for app_data in response['apps']:
            app = App(hockeyapp_obj=self,
                      app_id=str(app_data['public_identifier']),
                      id=str(app_data['id']),
                      title=app_data['title'],
                      bundle_id=str(app_data['bundle_identifier']),
                      platform=str(app_data['platform']))
            app.release_type = App.release_type_from_value(app_data['release_type'])
            app_list.append(app)
        return app_list

    def app(self, app_id):
        return App(hockeyapp_obj=self, app_id=app_id).read()

    def delete_app(self, app_id):
        app = App(hockeyapp_obj=self, app_id=app_id)
        app.delete()
