import unittest


from start import app


class SMFRTestCase(unittest.TestCase):

    ############################
    #### setup and teardown ####
    ############################

    @classmethod
    def setUpClass(cls):
        app.testing = True
        app.config['TESTING'] = True
        app.config['WTF_CSRF_ENABLED'] = False
        app.config['DEBUG'] = False
        cls.app = app.test_client()
