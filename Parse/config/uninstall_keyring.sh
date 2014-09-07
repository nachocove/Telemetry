#!/bin/sh

read -p "Enter the email username: " username
python -c "
import keyring
try:
    keyring.delete_password('NachoCove Telemetry', '$username')
except:
    print 'ERROR: fail to remove the password.'
"
