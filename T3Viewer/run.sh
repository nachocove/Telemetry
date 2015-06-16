#!/bin/sh
if [ ! -z "$1" ] ; then
   export PROJECT=$1
fi

export PYTHONPATH=$PYTHONPATH:../scripts:
python manage.py runserver 8081
