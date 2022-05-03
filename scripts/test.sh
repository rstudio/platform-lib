#!/usr/bin/env bash

# Exit with a non-zero code after checking all packages.
__EXITCODE=0

CURDIR=$(pwd)

MODULES=${MODULE:-$(find . -name go.mod | xargs -n1 dirname)}
DEF_TEST_ARGS=${DEF_TEST_ARGS:--short}

for MODULE in ${MODULES}; do
    cd ${MODULE}

    go test -buildvcs=false ${DEF_TEST_ARGS} ${TEST_ARGS[*]}
    result=$?
    if [[ $result -ne 0 ]]; then
      __EXITCODE=$result
    fi

    cd ${CURDIR}
done

exit $__EXITCODE;
