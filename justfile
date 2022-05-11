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
    dc="docker-compose -f docker/docker-compose.test.yml -p ${project}"

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

# Builds Go code using docker. Useful when using a MacOS or Windows native IDE. First,
# run `just build-build-env' to create the docker image you'll need.
build-docker:
    docker run {{ interactive }} --rm \
        -v {{justfile_directory()}}/:/build \
        -e GOCACHE=/platform-lib/.go-cache \
        -w /build \
        rstudio/platform-lib:lib-build just build

# Cleans Go build directory (out)
clean:
    rm -rf out/
    rm -rf data/
    rm -rf .chart/

# Remove docker images and clear build cache (useful to run before building cross platform images)
clean-docker:
    docker image rm rstudio/platform-lib:lib-build rstudio/platform-lib:lib-e2e
    docker builder prune

# Builds the docker image used for building Go code
# * args - Optional additional docker build args
build-build-env *args:
    DOCKER_BUILDKIT=1 docker build {{args}} -t rstudio/platform-lib:lib-build -f docker/bionic/Dockerfile docker/bionic

# Builds the docker image for e2e testing
# * args - Optional additional docker build args
build-e2e-env *args:
    docker build {{args}} --network host -t rstudio/platform-lib:lib-e2e -f .github/actions/test/Dockerfile .github/actions/test

# Creates a container for e2e testing
# * name - The container name
# * args - Additional docker create args
# * cmd - The command to run in the container
create-e2e-env name args cmd:
    #!/usr/bin/env bash
    set -euxo pipefail
    docker inspect {{name}} -f 'Found existing container' && docker rm {{name}} || echo "Created container"
    docker create {{args}} --rm \
        -v {{HOME_DIR}}/.aws:/root/.aws \
        -v /var/run/docker.sock:/var/run/docker.sock \
        -e HOME_DIR={{HOME_DIR}} \
        -e REMOTE_CONTAINERS=${REMOTE_CONTAINERS} \
        -w /test \
        --name {{name}} rstudio/platform-lib:lib-e2e {{cmd}}

# Copies test code to an e2e test container
# * name - The container name
copy-e2e-env name:
    docker cp test/. {{name}}:/test/
    docker cp out/. {{name}}:/test/assets/
    docker cp .github/actions/test/assets/.python-version {{name}}:/test/e2e/

# Start an interactive shell for e2e testing
start-e2e-env: (create-e2e-env "platform-lib-e2e-interactive" "-it" "/bin/bash") (copy-e2e-env "platform-lib-e2e-interactive")
    docker start -i platform-lib-e2e-interactive

# Run e2e tests (non-interactive)
test-e2e: (create-e2e-env "platform-lib-e2e" "" '/bin/bash -c "just test"') (copy-e2e-env "platform-lib-e2e")
    docker start -i platform-lib-e2e

# Stop a running container. This is only needed if something goes wrong.
stop-e2e-env:
    docker kill platform-lib-e2e-interactive

# generate Go dependencies' licenses file
licenses:
    ./scripts/go-licenses.sh

# enumerate latest tags for each module
tags: versions
versions:
    ./scripts/latest-tags.sh

# generate chart graph data and SVG chart
chart: (chart-data) (chart-svg)

# generate chart graph data
chart-data:
    ./scripts/generate-chart-data.sh

# generate SVG chart from graph data
chart-svg:
    ./scripts/generate-chart.sh
