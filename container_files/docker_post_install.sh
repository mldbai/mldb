#!/bin/bash

# This script is run in the docker before commiting it.

# Verbose!
set -x

set -e

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
groupadd -g 1234567 _mldb
useradd -u 1234567 -g 1234567 -c 'MLDB user' -M --home /mldb_data _mldb 
install -d -o _mldb -g _mldb /mldb_data
chown -R _mldb:_mldb /opt
mv /opt/local/version.json /opt/local/assets/www/version.json
