#!/bin/bash
cd {{MLDB_DATA_DIR}}
BIN=/opt/bin REMOTE_CREDENTIAL_PROVIDER=http://127.0.0.1:{{CREDENTIALSD_LISTEN_PORT}} /sbin/setuser _mldb /opt/bin/mldb_runner --http-listen-port {{MLDB_RUNNER_LISTEN_PORT}} --configuration-path {{MLDB_CONFIGURATION_PATH}} --static-doc-path {{MLDB_STATIC_ASSETS_PATH}}/www/doc/builtin --cache-dir {{MLDB_SSD_CACHE_PATH}} --plugin-directory {{MLDB_GLOBAL_PLUGINS_PATH}} --plugin-directory {{MLDB_LOCAL_PLUGINS_PATH}} {{MLDB_EXTRA_FLAGS}} 2>&1 | tee -a /var/log/mldb_runner.log
