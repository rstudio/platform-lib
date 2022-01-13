#!/usr/bin/env bash

# Exit with a non-zero code after checking all packages.
__EXITCODE=0

CURDIR=$(pwd)
# A temp file to store a list of packages
TMP=$(mktemp)
# A temp dir for all vendored code.
# TODO: Use `mktemp -d` and pass this directory to go-licenses.py as an arg or env var.
TMP_VEND=${CURDIR}/tmpvendor
mkdir -p ${TMP_VEND}

for MODULE in $(find . -name go.mod | xargs dirname); do
    echo "Generate vendor directory for module ${MODULE}"
    if [ ! -f "${MODULE}/go.mod" ] ; then
        echo "Skipping module ${MODULE}, as it does not contain go.mod."
        continue
    fi

    # vendor the module
    cd "${MODULE}"
    go mod vendor

    # Dump the information we need from go.mod
    cat go.mod \
      | awk '/\t/{ print $0 }' \
      | grep -E -v '(rstudio|indirect)' \
      | awk '{ print $1 }' \
    >> ${TMP}

    # Copy all vendored assets to the temporary vendor directory
    cp -R vendor/ ${TMP_VEND}/

    # Clean up since we don't vendor for a shared lib
    rm -rf vendor

    cd ${CURDIR}
done

# Dump and sort the list of all packages we generated above, and pipe
# the sorted list to the go-licenses.py script. This replaces the information
# in NOTICE.md.
cat ${TMP} \
  | sort -u \
  | ${CURDIR}/scripts/go-licenses.py \
> NOTICE.md

# Clean up the temporary assets.
rm ${TMP}
rm -rf ${TMP_VEND}

exit $__EXITCODE;
