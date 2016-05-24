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

if [ $(id -u $MLDB_USER) -eq 0 ]; then
    echo "" >&2
    echo "" >&2
    echo "" >&2
    echo "Aborting boot: cannot run MLDB as root" >&2
    echo "" >&2
    echo "Please use different values for MLDB_IDS" >&2
    echo "" >&2
    echo "" >&2
    echo "" >&2
    echo "" >&2
    exit 1
fi

if [ $(stat -c '%u' /mldb_data) -eq 0 ]; then
    echo "" >&2
    echo "" >&2
    echo "" >&2
    echo "Aborting boot: directory mapped to /mldb_data owned by root" >&2
    echo "" >&2
    echo "Expected owner uid from MLDB_IDS is: $(id -u $MLDB_USER)" >&2
    echo "" >&2
    echo "If the directory mapped to /mldb_data did not exist before launching MLDB," >&2
    echo "it is automatically created and owned by root. If this is what happened," >&2
    echo "please change the owner of the directory and relaunch." >&2
    echo "" >&2
    echo "" >&2
    echo "" >&2
    echo "" >&2
    exit 1
fi

if [ $(id -u $MLDB_USER) -ne $(stat -c '%u' /mldb_data) ]; then
    echo "" >&2
    echo "" >&2
    echo "" >&2
    echo "Aborting boot: bad owner on directory mapped to /mldb_data" >&2
    echo "" >&2
    echo "Apparent owner uid from within Docker container is: $(stat -c '%u' /mldb_data)" >&2
    echo "" >&2
    echo "Expected owner uid from MLDB_IDS is: $(id -u $MLDB_USER)" >&2
    echo "" >&2
    echo "" >&2
    echo "" >&2
    echo "" >&2
    exit 1
fi
