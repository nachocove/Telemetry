#!/bin/sh
if [ ! -z "$1" ] ; then
   export PROJECT=$1
fi
CWD=`dirname $0`
export PYTHONPATH=$PYTHONPATH:$CWD:$CWD/../scripts
python manage.py runserver

