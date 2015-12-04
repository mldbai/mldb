#!/bin/bash

/sbin/setuser _mldb /opt/bin/credentialsd --listen-port {{CREDENTIALSD_LISTEN_PORT}} --listen-host {{CREDENTIALSD_LISTEN_ADDR}} --credentials-path {{MLDB_CREDENTIALS_PATH}} 2>&1 | tee /var/log/credentialsd.log

