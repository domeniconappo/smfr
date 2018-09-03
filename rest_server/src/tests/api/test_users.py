import ujson as json

from smfrcore.models import User

from tests.api import ApiTest


class TestUserEndpoints(ApiTest):

    def test_signup_new_user(self):
        res = self.register_user('mail@test.com', 'password')
        self.assertEqual(res.status_code, 201)
        self.assertTrue('access_token' in json.loads(res.get_data()))

    def test_signup_existing_user(self):
        self.register_user('mail@test.com', 'password')
        res = self.register_user('mail@test.com', 'password')
        self.assertEqual(res.status_code, 409)

    def test_signin_correct_credentials(self):
        self.register_user('mail@test.com', 'password')
        res = self.login_user('mail@test.com', 'password')
        self.assertEqual(res.status_code, 200)
        self.assertTrue('access_token' in json.loads(res.get_data()))

    def test_signin_incorrect_credentials(self):
        self.register_user('mail@test.com', 'password')
        res = self.login_user('mail@test.com', 'password1')
        self.assertEqual(res.status_code, 401)

    def test_protected_wo_jwt(self):
        url = self.endpoints['users']
        res = self.api_get(url)
        self.assertEqual(res.status_code, 401)

    def test_protected_with_jwt(self):
        url = self.endpoints['users']
        self.register_user('test@test.com', 'password', 'Janet')
        res = self.register_user('mail@test.com', 'password', 'John')
        json_res = json.loads(res.get_data())
        auth_token = json_res['access_token']
        res = self.api_get(url, jwt=auth_token)
        json_res = json.loads(res.get_data())
        self.assertEqual(res.status_code, 200)

        users = {user['name'] for user in json_res['users']}
        self.assertEqual(len(users), 2)
        self.assertSetEqual(users, {'John', 'Janet'})

    def test_get_user(self):
        res = self.register_user('test@test.com', 'password', 'John')
        json_res = json.loads(res.get_data())
        auth_token = json_res['access_token']
        user_id = json_res['email']
        res = self.api_get('{}/{}'.format(self.endpoints['users'], user_id), jwt=auth_token)

        self.assertEqual(res.status_code, 200)
        self.assertEqual(json_res['email'], 'test@test.com')
        self.assertEqual(json_res['name'], 'John')

    def test_forbidden(self):
        user = User.create(email='test@test.com', password='testtest', name='John')
        other_user = User.create(email='test1@test.com', password='testtest', name='Janet')
        url = '{}/{}'.format(self.endpoints['users'], other_user.email)
        res = self.api_get(url, jwt=user.generate_auth_token(self.app))
        self.assertEqual(res.status_code, 403)
