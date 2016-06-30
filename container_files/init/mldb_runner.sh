#!/bin/bash

exec 2>&1  # stderr to stdout for logging purposes

cd {{MLDB_DATA_DIR}}
cat <<EOF

MLDB (version {{VERSION_NAME2}}) is starting up
EOF

export BIN=/opt/bin

exec /sbin/setuser _mldb \
    /opt/bin/mldb_runner \
        --http-listen-port {{MLDB_RUNNER_LISTEN_PORT}} \
        --credentials-path {{MLDB_CREDENTIALS_PATH}} \
        --static-assets-path {{MLDB_PUBLIC_HTML_PATH}}/resources \
        --static-doc-path {{MLDB_PUBLIC_HTML_PATH}}/doc \
        --cache-dir {{MLDB_SSD_CACHE_PATH}} \
        --plugin-directory {{MLDB_GLOBAL_PLUGINS_PATH}} \
        --plugin-directory {{MLDB_LOCAL_PLUGINS_PATH}} \
        --http-base-url "${HTTP_BASE_URL}" \
        {{MLDB_EXTRA_FLAGS}} \
        $MLDB_RUNNER_ARGS
