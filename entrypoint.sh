#!/bin/sh

python manage.py migrate --no-input

python manage.py collectstatic --no-input

uwsgi --strict --ini uwsgi.ini

#python etl/postgres_elastic_sync.py
