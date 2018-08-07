#!/bin/bash -e
find build/x86_64/obj/ -name "*.d" | xargs cat | grep '\.h:' | sed 's!:$!!' | sed 's!mldb/mldb/!mldb/!' | sort | uniq -c | sort -nr -k1 | head -n 100
