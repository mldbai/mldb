#!/bin/bash

install -o _mldb -d -m 750 /var/log/validator_api

install -d -o _mldb -g _mldb {{MLDB_VALIDATOR_DIR}}
export CONFIG=/etc/validator_api_config.json
/sbin/setuser _mldb /usr/local/bin/uwsgi \
    --socket 127.0.0.1:{{UWSGI_VALIDATOR_PORT}} \
    --master \
    --processes 8 \
    --logto=/var/log/validator_api/validator_api.log \
    --chdir=/opt/bin \
    --wsgi-file=/opt/bin/validator_api.wsgi \
    --pythonpath=/opt/lib/python2.7 \
    --lazy \
    --need-app
