import os
import ujson as json
import logging
from dateutil.parser import parse

from flask import Flask
from flask_bootstrap import Bootstrap

from smfrcore.client.conf import ServerConfiguration


logging.getLogger('urllib3').setLevel(logging.ERROR)
os.environ['NO_PROXY'] = ServerConfiguration.restserver_host


def create_app():
    application = Flask(__name__)
    application.secret_key = 'development key'
    Bootstrap(application)
    return application


app = create_app()

import views


def datetimeformat(value, fmt='%Y-%m-%d %H:%M'):
    """
    A jinja2 filter for dates/times
    :param value: a string like 2018-02-23T13:24:04+00:00
    :param fmt: output rendered format
    :return:
    """
    if value is None:
        return ''
    value = parse(value)
    return value.strftime(fmt)


def jsonfilter(value):
    return json.dumps(value)


app.jinja_env.filters['json'] = jsonfilter
app.jinja_env.filters['datetimeformat'] = datetimeformat
app.jinja_env.filters['zip'] = zip
