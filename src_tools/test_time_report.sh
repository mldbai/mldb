#!/bin/bash -e
find build/x86_64/tests/ -name "*.timing" | xargs grep 'User' | sed 's!build/[^\]*/tests/\(.*\)\.timing:\tUser time (seconds):!\1\t!' | sort -nr -k2 | awk -F '\t' '{ printf("%7.2f\t%s\n", $2, $1); }'
