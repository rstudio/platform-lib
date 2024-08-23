export REMOTE_CONTAINERS := env_var_or_default('REMOTE_CONTAINERS', 'false')

# When running in VS Code in a docker container, this is set to the home directory
# of the host machine. Since bind mount `/var/run/docker.sock` from the host to the
# VSCode docker container, we can kick off additional docker processes from VSCode,
# but any directories we mount need to know the host's home directory.
HOME_DIR := env_var_or_default('HOST_HOME', env_var('HOME'))

HOSTNAME := env_var_or_default('HOSTNAME', '')

# are we attached to a terminal?
interactive := `tty -s && echo "-it" || echo ""`

# Runs Go unit tests
test *args:
    #!/usr/bin/env bash
    set -euo pipefail

    test_args="{{ args }}"
    test_args=${test_args:-./...}

    TEST_ARGS="$test_args" ./scripts/test.sh

# Runs Go unit tests with docker-compose
test-integration *args:
    #!/usr/bin/env bash
    set -euo pipefail

    # use a randomized project name to allow simultaneous test runs
    project="platform-lib-it-$(openssl rand -hex 4)"
    dc="docker compose -f docker/docker-compose.test.yml -p ${project}"

    function cleanup() {
        ${dc} logs --no-color > test-integration-${project}.log
        ${dc} down -v --remove-orphans
    }
    trap cleanup EXIT

    test_args="{{ args }}"
    test_args=${test_args:-./...}
    ${dc} run \
        -e TEST_ARGS="$test_args" \
        -e GOCACHE=/platform-lib/.go-cache \
        -e DEF_TEST_ARGS="-v" \
        -e MODULE=${MODULE:-""} \
        lib ./scripts/test.sh

# Checks Go code
vet:
    ./scripts/go-vet.sh

# Builds Go code natively.
build:
    cd examples && \
    go build -buildvcs=false -o ../out/ ./...

# run linters
lint:
    ./scripts/fmt-check.sh
    ./scripts/header-check.sh
    ./scripts/test-wiring.sh

# Opens a shell in the dev docker container.
bash:
    docker run {{ interactive }} --rm \
        -v {{justfile_directory()}}/:/build \
        -e GOCACHE=/platform-lib/.go-cache \
        -w /build \
        rstudio/platform-lib:lib-build /bin/bash

# Cleans Go build directory (out)
clean:
    rm -rf out/
    rm -rf data/
    rm -rf .chart/

# generate Go dependencies' licenses file
licenses:
    ./scripts/go-licenses.sh

# enumerate latest tags for each module
tags: versions
versions:
    ./scripts/latest-tags.sh
