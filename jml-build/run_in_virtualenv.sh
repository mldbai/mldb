#!/bin/bash
# Run the rest of the command line in a virtualenv
# The first arg is the virtualv

env | grep PYTHONPATH

set -e
venv=$1
shift
source $venv/bin/activate
#echo "PYTHONPATH=" $PYTHONPATH

$@
