#!/usr/bin/env bash

# Exit with a non-zero code after checking all packages.
__EXITCODE=0

CURDIR=$(pwd)

for MODULE in $(find . -name go.mod | xargs -n1 dirname); do
    echo "Vetting module ${MODULE}"
    if [ ! -f "${MODULE}/go.mod" ] ; then
        echo "Skipping module ${MODULE}, as it does not contain go.mod."
        continue
    fi

    if [ "${MODULE}" = "generate" ] ; then
        echo "Skipping module ${MODULE}, as its Go files are excluded by build constraints."
        continue
    fi

    cd "${MODULE}"

    # Vet
    go vet ./...
    result=$?
    if [[ $result -ne 0 ]]; then
      __EXITCODE=$result
    fi

    cd ${CURDIR}
done

exit $__EXITCODE;
