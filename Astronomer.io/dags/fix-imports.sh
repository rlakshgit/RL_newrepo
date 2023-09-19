#!/usr/bin/env bash

RC=0
for file in $(find . -name "*.py"); do
    echo "Checking $file"
    sed -f fix-imports.txt $file > $file.tmp
    if [ $? -eq 0 ]; then
        mv $file.tmp $file
    else
        echo "Failed to fix imports in $file"
        # rm $file.tmp
        RC=1
    fi
done
exit $RC
