from flask import Flask
from flask_bootstrap import Bootstrap


def create_app():
    application = Flask(__name__)
    application.secret_key = 'development key'
    Bootstrap(application)
    return application


app = create_app()

import views
