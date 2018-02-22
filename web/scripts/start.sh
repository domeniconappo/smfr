#!/usr/bin/env bash

gunicorn -b 0.0.0.0:8888 --workers 1 --error-logfile - --access-logfile - --reload --name webapp "start:app"
