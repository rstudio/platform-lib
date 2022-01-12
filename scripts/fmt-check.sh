#!/usr/bin/env bash

# Exit with a non-zero code after checking all packages.
__EXITCODE=0

for MODULE in "." ; do
    echo "Checking fmt for module ${MODULE}"
    if [ ! -f "${MODULE}/go.mod" ] ; then
        echo "Skipping module ${MODULE}, as it does not contain go.mod."
        continue
    fi

    if [ "${MODULE}" = "generate" ] ; then
        echo "Skipping module ${MODULE}, as its Go files are excluded by build constraints."
        continue
    fi

    cd "${MODULE}"

    # Collect a list of go package directories ('go list' skips vendor packages).
    PACKAGES=$(go list -f '{{.Dir}}' ./...)
    for pkg in ${PACKAGES}; do
        # If gofmt changes anything, tell the user
        if $(gofmt -d -s "${pkg}"/*.go | read -r SCRATCH); then
            echo "Go source in package ${pkg} needs formatting"
            gofmt -d -s "${pkg}"/*.go
            __EXITCODE=1
        fi
    done

    cd ..
done

exit $__EXITCODE;
