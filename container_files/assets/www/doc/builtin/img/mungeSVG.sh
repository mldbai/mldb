#!/bin/bash

chmod o+r *svg
grep -Lr _mldb *svg | xargs sed -i -r -e 's/viewBox="0.0 0.0 ([^ ]*) ([^"]*)"/ \0 width="\1" height="\2" _mldb="1"/'
