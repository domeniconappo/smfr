#!/usr/bin/env bash

# Used only in development, to reload uwsgi
docker exec web touch /etc/uwsgi/uwsgi.ini
echo Webapp reloaded
