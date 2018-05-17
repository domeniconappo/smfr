import unittest
import warnings

from start import app


warnings.simplefilter("ignore")


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
        cls.app = app
