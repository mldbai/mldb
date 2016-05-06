#!/bin/bash

#set -x

# Adjust the _mldb user's UID and GID based on MLDB_IDS env variable
# Format of that variable must be the output of the /bin/id command:
#     "uid=%u(%s) gid=%u(%s)..."

set -e

MLDB_USER="_mldb"
if [ -n "$MLDB_IDS" ]; then
    IDS=$(echo "$MLDB_IDS" | sed 's/uid=\([[:digit:]]*\).* gid=\([[:digit:]]*\).*/\1:\2/g')
    MLDB_UID=$(echo $IDS | awk -F: '{print $1}')
    MLDB_GID=$(echo $IDS | awk -F: '{print $2}')
else
    MLDB_UID=$(id -u _mldb)
    MLDB_GID=$(id -g _mldb)
fi

#echo "Mapping $MLDB_USER to uid $MLDB_UID" >&2
/usr/sbin/usermod --non-unique --uid $MLDB_UID $MLDB_USER

#echo "Mapping $MLDB_USER to gid $MLDB_GID" >&2
/usr/sbin/groupmod --non-unique -g "$MLDB_GID" $MLDB_USER

# Adjust perms for a few directories
# FIXME: these should not be writable outside of dev environment
chown -R $MLDB_USER:$MLDB_USER /opt

if [ -z "$(find /mldb_data -maxdepth 0 -user $(id -u $MLDB_USER))" ]; then
    echo "" >&2
    echo "" >&2
    echo "" >&2
    echo "Aborting boot: bad permissions on /mldb_data (not owned by $(id $MLDB_USER))" >&2
    echo "" >&2
    echo "" >&2
    echo "" >&2
    exit 1
fi
