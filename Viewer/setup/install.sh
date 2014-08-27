#!/bin/sh

function check_exit_code ()
{
	if [ $? -ne 0 ] ; then
		echo "ERROR: $1"
		exit 1
	fi
}

if [ "`whoami`" != "root" ] ; then
	echo "ERROR: setup.sh not run with sudo.\n"
	echo "USAGE: sudo ./install.sh"
	exit 1
fi

which curl > /dev/null
check_exit_code "cannot find curl."

curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
check_exit_code "cannot download get-pip.py."

python get-pip.py
check_exit_code "cannot install pip."
rm -f get-pip.py

pip install Django
check_exit_code "cannot install Django."

django_version=`python -c "import django ; print django.get_version()"`
echo "Django $django_version is installed."