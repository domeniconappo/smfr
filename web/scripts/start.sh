#!/usr/bin/env bash

gunicorn -b 0.0.0.0:8888 --workers 4 --worker-class gevent --reload --name webapp "start:app"
