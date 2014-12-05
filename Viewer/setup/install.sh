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

echo "Downloading and installing/updating easy_install"
# get and install easy_install/setuptools
curl -s https://bootstrap.pypa.io/ez_setup.py -o - | python > /dev/null
check_exit_code "cannot download/run ez_setup.py."

# use easy_install to install/update pip
easy_install -q -U pip
check_exit_code "cannot easy_install pip"

echo "Installing boto"
pip -q install boto
check_exit_code "cannot install boto."

echo "Installing Django"
pip -q install Django
check_exit_code "cannot install Django."

django_version=`python -c "import django ; print django.get_version()"`
echo "Django $django_version is installed."