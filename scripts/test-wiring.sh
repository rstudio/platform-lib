#!/usr/bin/env bash

# This script analyizes each go package to ensure that testing is wired up properly
#   1. gocheck is hooked into testing.T (via `func TestMain(t *testing.T) { check.TestingT(t) }`)
#   2. Each defined suite is wired into gocheck (via `var _ = check.Suite(&XXXXXXX{})`)

exitcode=0

# find suites that are defined but not enabled
unplugged_suites() {
    # Get a list of suite structs defined in this package.
    #
    # We ignore suites which begin with lowercase letters; those are not
    # public and used when defining full suites.
    defined=$(grep -shE 'type [[:upper:]].*Suite struct' $1/*_test.go | \
                  awk '{ print $2 }' | \
                  sort)

    # Get a list of suite structs connected to check.Suite. These lines look
    # like:
    #    var _ = check.Suite(&XXXXXXXXX{})
    #
    # We ignore NothingSuite because it is used as a placeholder in packages
    # without test coverage.
    active_check=$(grep -sh "check.Suite" "$1"/*_test.go | \
                 awk -F "&" {' print $2 '} | \
                 awk -F "{" '{ print $1 }' | \
                 grep -v NothingSuite | \
                 sort)

    # Get a list of suite structs connected to testify.suite. These lines look
    # like:
    #
    #    suite.Run(t, &CreateEndpointSuite{})
    active_testify=$(grep -sh "suite.Run" "$1"/*_test.go | \
                 awk -F "&" {' print $2 '} | \
                 awk -F "{" '{ print $1 }' | \
                 sort)


    # Suites that are connected multiple times.
    doubles=$(echo $active_check $active_testify | tr " " "\n" | uniq -d)
    if [[ "x$doubles" != "x" ]]; then
        echo "Error: package $1 has test suites connected more than once: ${doubles}"
        exitcode=1
    fi

    # Suites that are not connected. Discovered by concatenating defined and
    # active together and then looking for anything that is NOT repeated.
    disconnected=$(echo $active_check $active_testify $defined | tr " " "\n" | sort | uniq -u)
    if [[ "x$disconnected" != "x" ]]; then
        exitcode=1
        echo "Error: package $1 has test suites that are disconnected: ${disconnected}"
    fi
}

# find packages that don't hook up check to either check.TestingT (for check) or
# suite.Run (for testify)
testing_enabled() {
    count=$(grep -hsc -e "check.TestingT" -e "suite.Run" "$1"/*_test.go | awk '{s+=$1} END {printf "%.0f", s}')
    if [[ $count == "0" ]]; then
        echo "Error: package $1 is missing a call to check.TestingT or suite.Run"
        exitcode=1
    fi
}

CURDIR=$(pwd)

for MODULE in $(find . -name go.mod | xargs dirname); do
    echo "Checking test wiring for module ${MODULE}"
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
        unplugged_suites $pkg
        testing_enabled $pkg
    done
    cd ${CURDIR}
done


exit $exitcode
