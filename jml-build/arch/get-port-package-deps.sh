#!/bin/bash
#
# usage
#
# get-port-package-deps.sh <packageName> <arch>
#
# Installs the files in the given packageName for the given arch under
# the given directory.

#set -x  # debug mode
set -e

ARCH=$2

if [ $ARCH "=" "aarch64" ]; then
    ARCH="arm64"
elif [ $ARCH "=" "arm" ]; then
    ARCH="armhf"
fi

DEPS=`apt-cache -o APT::Architecture=$ARCH depends $1 | grep 'Depends:' | grep -v '<' | sed 's/[^:]*://' | sed "s/:$ARCH//" | sort | uniq`

echo PACKAGE_DEPS_$1:='$('filter-out '$('PORT_BLACKOUT_PACKAGES')',$DEPS')'
echo PORT_ALL_DEPS_COMPUTED+=' $('PACKAGE_DEPS_$1')'
echo '$(BUILD)/$(ARCH)/osdeps/tmp/deps-'$1'.mk:	$(foreach dep,$(PACKAGE_DEPS_'$1'),$(BUILD)/$(ARCH)/osdeps/tmp/deps-$(dep).mk)  $$(warning deps for $(1) are $$(PACKAGE_DEPS_$(1)))'
echo port_deps: '$(foreach dep,$(PACKAGE_DEPS_'$1'),$(BUILD)/$(ARCH)/osdeps/tmp/installed-$(dep))'
