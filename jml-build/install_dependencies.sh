#!/bin/bash
. $VIRTUALENV/bin/activate
pip install -r python_requirements_$OSNAME.txt -c python_constraints.txt
