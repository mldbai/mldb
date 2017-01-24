#!/bin/bash
# Copyright (c) 2015 mldb.ai inc.  All rights reserved.
set -e
set -x

progname=$(basename $0)

CIDFILE=$(mktemp -u -t $progname.cid.XXXXXX)  # Race me!
BASE_IMG="quay.io/mldb/baseimage:0.9.17"
IMG_NAME=${IMG_NAME:="quay.io/mldb/mldb_base:14.04"}

BUILD_DOCKER_DIR="/mnt/build"

function on_exit {
  if [ -n "$CIDFILE" ]; then
      rm -f "$CIDFILE" || true
  fi
}
trap on_exit EXIT

function usage {
cat <<EOF >&2
$progname [-b base_image] [-i image_name] [-w pip_wheelhouse_url]

    -b base_image               Base image to use ($BASE_IMG)
    -i image_name               Name of the resulting image ($IMG_NAME)
    -w pip_wheelhouse_url       URL to use a a pip wheelhouse

EOF
}

while getopts "b:p:i:w:" opt; do
  case $opt in
    b)
      BASE_IMG="$OPTARG"
      ;;
    i)
      IMG_NAME="$OPTARG"
      ;;
    w)
      PIP_WHEELHOUSE="-f $OPTARG"
      ;;
    *)
      usage
      exit
      ;;
  esac
done

docker run -i --cidfile "$CIDFILE" -v $PWD:$BUILD_DOCKER_DIR:ro "$BASE_IMG" bash -c 'cat > /tmp/command-to-run && chmod +x /tmp/command-to-run && exec /tmp/command-to-run' <<EOF
set -e
set -x
echo "deb http://us-east-1.ec2.archive.ubuntu.com/ubuntu/ trusty main universe" >/etc/apt/sources.list
echo "deb http://security.ubuntu.com/ubuntu/ trusty-security universe main multiverse restricted" >> /etc/apt/sources.list
echo "deb http://us-east-1.ec2.archive.ubuntu.com/ubuntu/ trusty-updates universe main multiverse restricted" >> /etc/apt/sources.list
echo "deb http://us-east-1.ec2.archive.ubuntu.com/ubuntu/ trusty-backports universe main multiverse restricted" >> /etc/apt/sources.list
apt-get update
apt-get upgrade -y
apt-get install -y python-software-properties software-properties-common
add-apt-repository -y ppa:nginx/stable
apt-get update

#####################
# MLDB dependencies #
#####################
apt-get install -y \
    bash \
    binutils \
    nginx \
    vim-tiny \
    libboost-serialization1.54.0 \
    libboost-program-options1.54.0 \
    libboost-system1.54.0 \
    libboost-regex1.54.0 \
    libboost-locale1.54.0 \
    libboost-date-time1.54.0 \
    libboost-iostreams1.54.0 \
    libboost-python1.54.0 \
    libboost-filesystem1.54.0 \
    libgoogle-perftools4 \
    liblzma5 \
    libbz2-1.0 \
    libcrypto++9 \
    libcurlpp0 \
    libcurl3 \
    libssh2-1 \
    libpython2.7 \
    libicu52 \
    liblapack3 \
    libblas3 \
    libevent-1.4-2 \
    libidn11 \
    python-tk \
    libarchive13 \
    unzip \
    unrar-free \
    libstdc++6 \
    libpq5 \
    libyaml-cpp0.5

#######################
# Python dependencies #
#######################
apt-get install -y python-pip
pip install -U pip setuptools || true  # https://github.com/pypa/pip/issues/3045
pip2 install -U $PIP_WHEELHOUSE -r $BUILD_DOCKER_DIR/python_requirements_mldb_base.txt -c $BUILD_DOCKER_DIR/python_constraints.txt

# Drop the statically linked Python images
rm -f /usr/lib/python2.7/config-x86_64-linux-gnu/*.a

# Make sure en_US.UTF-8 is available
locale-gen en_US.UTF-8

# Python stuff cleanup
# rm .pycs and rebuild them on boot
find /usr/local/lib/python2.7/dist-packages -name '*.pyc' -delete
install -m 555 $BUILD_DOCKER_DIR/mldb/mldb_base/rebuild_pycs.py  /usr/local/bin/
cat >/etc/my_init.d/99-rebuild_pycs.sh <<BIF
#!/bin/bash

nice /usr/local/bin/rebuild_pycs.py &
BIF
chmod +x /etc/my_init.d/99-rebuild_pycs.sh

# Disable syslog-ng output to console
rm -rf /etc/service/syslog-forwarder

# Final cleanup
apt-get purge -y vim 'language-pack-*' iso-codes python-software-properties software-properties-common rsync cpp gcc gcc-4.8 cpp-4.8
apt-get autoremove -y --purge
apt-get clean -y

# Remove extra data...
rm -rf /usr/local/lib/python2.7/dist-packages/bokeh/server/tests/data
rm -rf /usr/local/lib/python2.7/dist-packages/matplotlib/tests/baseline_images
# Strip libs
find /usr/local/lib/python2.7 -type f -name "*so" -exec strip {} \;

rm -rf /root/.cache /var/lib/apt/lists/* /tmp/* /var/tmp/* 2>/dev/null || true
rm -rf /usr/share/man*  2>/dev/null || true
# drop everything but keep copyright/licenses
find /usr/share/doc -type f ! -name copyright -delete 2>/dev/null || true
find /usr/share/doc -type d -empty -delete
# that's magnificent...
find /var/log -type f -name '*.log' -exec bash -c '</dev/null >{}' \;
EOF

CID=$(cat "$CIDFILE")
echo $CID
docker commit $CID "$IMG_NAME"
docker rm $CID
