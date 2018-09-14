#!/usr/bin/env bash

cd /restserver
flask db upgrade
gunicorn -b 0.0.0.0:5555 --workers 4 --log-level warning --timeout 240 -k gevent --reload --access-logfile - --name restserver --preload start:app
