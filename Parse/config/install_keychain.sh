#!/bin/sh

# Install python package keyring
easy_install keyring

# Install the password into keychain
read -p "Enter the email username: " username
read -s -p "Enter the email password: " password
python -c "
import keyring
keyring.set_password('NachoCove Telemetry', '$username', '$password')
"

