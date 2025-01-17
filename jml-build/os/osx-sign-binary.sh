#!/bin/bash
# This script is used to sign a binary on OSX so that it can be debugged and dump core.
#echo "Signing $1"
set -e
#set -x
codesign -s - -f --entitlements mldb/jml-build/os/mldb.debug.entitlements.plist $1 > $1.codesign.log 2>&1 
#codesign -vvv $1 >> $1.codesign.log 2>&1
