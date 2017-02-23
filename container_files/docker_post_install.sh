#!/bin/bash

# This script is run in the docker before commiting it.

# Verbose!
set -x
set -e

MLDB_UID=56789

while getopts "s" opt; do
  case $opt in
    s)
      RUN_STRIP=1
      ;;
  esac
done

# Make sure we don't have the default nginx site lying around
rm -f /etc/nginx/sites-enabled/default || true

if [ -n "$RUN_STRIP" ]; then
    echo "Running strip on /opt/bin/* and /opt/lib/*. Ignore errors" >&2
    # yeah that's crude...
    find /opt/bin -type f ! -name '.*py' ! -name '*txt' -exec strip {} \; 2>/dev/null || true
    find /opt/lib -type f -name '*so' -exec strip {} \;  || true
fi


##############
# _mldb user #
##############
groupadd -g $MLDB_UID _mldb
useradd -u $MLDB_UID -g $MLDB_UID -c 'MLDB user' -s /bin/bash -M --home /mldb_data _mldb 
install -d -o _mldb -g _mldb /mldb_data
chown -R _mldb:_mldb /opt
