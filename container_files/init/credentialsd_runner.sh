#!/bin/bash

exec 2>&1  # stderr to stdout for logging purposes

exec /sbin/setuser _mldb /opt/bin/credentialsd --listen-port {{CREDENTIALSD_LISTEN_PORT}} --listen-host {{CREDENTIALSD_LISTEN_ADDR}} --credentials-path {{MLDB_CREDENTIALS_PATH}}

