import ujson as json

from tests.db import DBTest


class TestMixin:
    def api_get(self, path, jwt=None):
        """
        Helper method for client get requests
        :param path: str, endpoint /api/users/
        :param jwt: str XXXXX.YYYYY.ZZZZZZ
        """
        with self.client as client:
            headers = None
            if jwt:
                auth_header = 'Bearer {}'.format(jwt)
                headers = {'Authorization': auth_header}
            return client.get(path, headers=headers, content_type='application/json')

    def register_user(self, email, password, name=''):
        with self.client as client:
            return client.post(
                self.endpoints['users'],
                data=json.dumps(dict(
                    email=email,
                    password=password,
                    name=name
                )),
                content_type='application/json',
            )

    def login_user(self, email, password):
        with self.client as client:
            return client.post(
                self.endpoints['signin'],
                data=json.dumps(dict(
                    email=email,
                    password=password
                )),
                content_type='application/json',
            )


class ApiTest(DBTest, TestMixin):
    endpoints = {
        'users': '/1.0/users',
        'signin': '/1.0/users/signin',
    }
