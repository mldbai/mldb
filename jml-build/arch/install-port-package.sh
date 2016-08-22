#!/bin/bash
#
# usage
#
# install-port-package.sh <packageName> <arch> <dir>
#
# Installs the files in the given packageName for the given arch under
# the given directory.

#set -x  # debug mode
set -e

ARCH=$2

if [ $ARCH "=" "aarch64" ]; then
    ARCH="arm64"
fi


FILENAME=`apt-cache show $1:$ARCH | grep '^Filename' | head -n1 | sed 's/^Filename: //'`
BASENAME=`basename $FILENAME`
echo ARCH=$ARCH
echo FILENAME=$FILENAME
echo BASENAME=$BASENAME

mkdir -p $3/tmp

if [ -f $3/tmp/$BASENAME ]; then
    echo "skipping download as already exists"
else
    (cd $3/tmp && apt-get download $1:$ARCH)
fi

# Extract from the downloaded file
if (ar t $3/tmp/$BASENAME | grep data.tar.xz) then
    ar p $3/tmp/$BASENAME data.tar.xz | tar Jxv --overwrite -C $3
else
    ar p $3/tmp/$BASENAME data.tar.gz | tar zxv --overwrite -C $3
fi

echo "installed $1 for architecture $ARCH in $3"
