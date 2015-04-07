Things to install:

Mac OS X
========

openssl
-------
https://www.openssl.org/source/

```
cd openssl-1.0.2a
./Configure darwin64-x86_64-cc -shared
make
sudo make install
```

PCRE (for swig)
---------------
http://www.pcre.org/
http://sourceforge.net/projects/pcre/files/pcre/

```
cd pcre-8.36
./configure
make
sudo make install
```

Swig (for M2Crypto)
-------------------

NOTE: Need to use swig 3.0.4, NOT 3.0.5

http://www.swig.org/download.html

```
cd swig-3.0.4
./configure
make
sudo make install
```

Python Packages
===============

pip
---
sudo easy_install pip

packages
--------
sudo pip install django requests boto boto3 awscli python-dateutil keyring 

M2Crypto
--------
http://chandlerproject.org/Projects/MeTooCrypto#Downloads

```
cd M2Crypto-0.21.1
python setup.py build build_ext --openssl=/usr/local/ssl
sudo python setup.py install build_ext --openssl=/usr/local/ssl
```
