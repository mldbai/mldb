#!/bin/sh

set -e
set -x

rm -f platform-deps.cid
docker run -i -cidfile platform-deps.cid -v ~/local:/tmp/local -v `pwd`:/tmp/build quay.io/datacratic/ubuntu-base sh -c 'cat > /tmp/command-to-run && chmod +x /tmp/command-to-run && exec /tmp/command-to-run' <<EOF
set -e
set -x
cp -r -d /tmp/local/* /usr/local/
rm -rf /usr/local/*virtualenv
ldconfig
ls /usr/local/*
EOF
CID=`cat platform-deps.cid`
echo $CID
docker commit $CID quay.io/datacratic/platform-deps
docker rm $CID
rm -f platform-deps.cid
