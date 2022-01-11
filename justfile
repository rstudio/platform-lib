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
    go test -short ${test_args[*]}

# Runs Go unit tests with docker-compose
test-integration *args:
    #!/usr/bin/env bash
    set -euo pipefail

    # When run in GitHub Actions, the following is set to 1
    export AD_CI=${AD_CI:-0}

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
    ${dc} run lib \
        go test -v ${test_args[*]}

# Checks Go code
vet:
    go vet ./...

# Builds Go code natively.
build:
    go build -o out/ ./...

# Builds Go code using docker. Useful when using a MacOS or Windows native IDE. First,
# run `just build-build-env' to create the docker image you'll need.
build-docker:
    docker run {{ interactive }} --rm \
        -v {{justfile_directory()}}/:/build \
        -w /build \
        rstudio/platform-lib:lib-build go build -o out/ ./...

# Cleans Go build directory (out)
clean:
    rm -rf out/

# Builds the docker image used for building Go code
build-build-env:
    docker build -t rstudio/platform-lib:lib-build -f docker/bionic/Dockerfile docker/bionic

# Builds the docker image for e2e testing
build-e2e-env:
    docker build --network host -t rstudio/platform-lib:lib-e2e -f .github/actions/test/Dockerfile .github/actions/test

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
    go mod vendor
    cat go.mod \
    | awk '/\t/{ print $0 }' \
    | grep -E -v '(rstudio|indirect)' \
    | awk '{ print $1 }' \
    | sort -u \
    | ./scripts/go-licenses.py \
    > NOTICE.md
    rm -rf vendor
