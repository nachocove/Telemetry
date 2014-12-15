#!/bin/sh
if [ ! -z "$1" ] ; then
   export PROJECT=$1
fi
python manage.py runserver
