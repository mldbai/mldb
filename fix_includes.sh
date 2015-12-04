#!/bin/bash

# usage: fix_includes old_directory new_directory include1.h include2.h ...

OLD=$1
NEW=$2

shift 2

for header in $@; do
    echo "moving header $header"

    FILES=`find . -name "*.h" -print -or -name "*.cc" -print | xargs grep -l '#include.*["/]'"$header"  2>/dev/null`

    echo $FILES

    for file in $FILES; do
        if echo $file | grep "^./$NEW/" > /dev/null && grep '#include .*"'$header'"' $file > /dev/null; then
            echo "file $file is in the directory and includes directly"
            continue;
        fi
        if grep '#include .*"'$header'"' $file > /dev/null; then
            echo "included directly in $file";
            sed -i -e 's#"'$header'"#"'$NEW/$header'"#' $file
        fi
        if grep '#include .*"'$OLD/$header'"' $file > /dev/null; then
            echo "incldued with path in $file";
            sed -i -e 's#"'$OLD/$header'"#"'$NEW/$header'"#' $file
        fi
    done
done

#'"'"$1value_manager.h"' | xargs sed -i -e 's#"value_manager.h"#"type_system/value_manager.h"#