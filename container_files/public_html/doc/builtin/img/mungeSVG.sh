#!/bin/bash

# this fixes permissions
chmod o+r *svg

# grep will filter out SVGs that are already processed because they are tagged with _mldb="1"
# sed will get dimenstions from viewbox attribute and create width and height attributes and dummy _mldb attribute
grep -Lr _mldb *svg | xargs sed -i -r -e 's/viewBox="0.0 0.0 ([^ ]*) ([^"]*)"/ \0 width="\1" height="\2" _mldb="1"/'
