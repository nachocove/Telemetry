#!/bin/sh
if [ -z "$1" ] ; then
    echo "USAGE: run.sh <project name>"
    echo "        project name: 'beta' or 'dev'. See projects.cfg."
    exit 1
fi

PROJECT=$1 python manage.py runserver
