#!/bin/bash
#
# usage
#
# install-port-package.sh <packageName> <arch> <dir>
#
# Installs the files in the given packageName for the given arch under
# the given directory.

set -x  # debug mode
set -e

ARCH=$2

if [ $ARCH "=" "aarch64" ]; then
    ARCH="arm64"
elif [ $ARCH "=" "arm" ]; then
    ARCH="armhf"
fi

BASENAME=`apt-get download --print-uris $1:$ARCH | awk '{ print $2; }' | head -n1`
echo ARCH=$ARCH
echo BASENAME=$BASENAME

mkdir -p $3/tmp

if [ -f $3/tmp/$BASENAME ]; then
    echo "skipping download as already exists"
else
    (cd $3/tmp && apt-get download $1:$ARCH)
fi

# Extract from the downloaded file
DATAFILE=`ar t $3/tmp/$BASENAME | grep data\.tar\.`
echo DATAFILE=$DATAFILE

if [ $DATAFILE = "data.tar.xz" ]; then
    ar p $3/tmp/$BASENAME data.tar.xz | tar Jxv --overwrite -C $3
elif [ $DATAFILE = "data.tar.gz" ]; then
    ar p $3/tmp/$BASENAME data.tar.gz | tar zxv --overwrite -C $3
elif [ $DATAFILE = "data.tar.bz2" ]; then
    ar p $3/tmp/$BASENAME data.tar.bz2 | tar jxv --overwrite -C $3
else
    echo "couldn't determine compression for data file $DATAFILE"
    exit 1
fi

#http://stackoverflow.com/questions/762348/how-can-i-exclude-all-permission-denied-messages-from-find
# Find any libraries that link to /lib in the development headers, and fix
# them up to link to ../../../lib so that they don't assume an absolute
# path anymore.
find -L $3/usr/lib ! -readable -prune -name "*.so*" -type 'l' -lname '/lib/*' -printf '../../../%l\0%p\0' | (xargs -0 -n2 ln -sf || true)

echo "installed $1 for architecture $ARCH in $3"
