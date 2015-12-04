#!/bin/sh

set -e
set -x

#pip bundle -r python_requirements.txt pip_bundle.zip

rm -f tmp/docker_dependencies.cid
docker run -i -cidfile tmp/docker_dependencies.cid -v ~/local:/tmp/local -v `pwd`:/tmp/build quay.io/datacratic/platform-deps sh -c 'cat > /tmp/command-to-run && chmod +x /tmp/command-to-run && exec /tmp/command-to-run' <<EOF
set -e
set -x
#virtualenv /usr/local/
mkdir -p /opt/lib/python2.7/site-packages/
cp -r /tmp/build/virtualenv/lib/python2.7/site-packages/* /opt/lib/python2.7/site-packages/
echo "/opt/lib/python2.7/site-packages" > /usr/lib/python2.7/dist-packages/local.pth
echo "/opt/lib/" > /usr/lib/python2.7/dist-packages/optlib.pth
cp -r /tmp/build/node_modules /opt
#pip install /tmp/build/pip_bundle.zip
EOF
CID=`cat tmp/docker_dependencies.cid`
echo $CID
docker commit $CID quay.io/datacratic/dependencies
docker rm $CID
rm -f tmp/docker_dependencies.cid
