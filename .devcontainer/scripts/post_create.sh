#!/bin/bash

echo "Adding and activating ssh keys to container..."

# Start ssh-agent if not already running
if [ -z "$SSH_AUTH_SOCK" ]; then
    eval "$(ssh-agent -s)"
fi

cd $HOME

# Add all non .pub keys to ssh-agent
if [ -d ".ssh" ]; then
    echo "Adding SSH keys to ssh-agent..."
    find ".ssh" -type f -not -name "*.pub" -not -name "known_hosts" -not -name "config" -not -name "authorized_keys" | while read -r keyfile; do
        # Check if file is a valid private key
        ssh-keygen -l -f "$keyfile" >/dev/null 2>&1
        if [ $? -eq 0 ]; then
            ssh-add "$keyfile" 2>/dev/null
            if [ $? -eq 0 ]; then
                echo "Added key: $keyfile"
            fi
        fi
    done
fi

# If there are no packages installed yet, install them
if [ ! -d "dbt_packages" ]; then
    dbt deps
fi