#!/usr/bin/env bash

# Exit with a non-zero code after checking all packages.
__EXITCODE=0

# Matches a copyright
COPYRIGHT_REGEX="// Copyright \(C\) [0-9]{4} by (RStudio|Posit Software), PBC"

# Collect a list of go files.
for gofile in $(find . -name *.go); do
    # Copyright is always the third line in the file.
    copyright=$(cat ${gofile} | head -3 | tail -1)

    # Check copyright against regex
    if ! [[ "${copyright}" =~ $COPYRIGHT_REGEX  ]]; then
        echo "Copyright notice in ${gofile} needs attention."
        __EXITCODE=1
    fi
done

exit $__EXITCODE;
