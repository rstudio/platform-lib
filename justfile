export REMOTE_CONTAINERS := env_var_or_default('REMOTE_CONTAINERS', 'false')

# When running in VS Code in a docker container, this is set to the home directory
# of the host machine. Since bind mount `/var/run/docker.sock` from the host to the
# VSCode docker container, we can kick off additional docker processes from VSCode,
# but any directories we mount need to know the host's home directory.
HOME_DIR := env_var_or_default('HOST_HOME', env_var('HOME'))

HOSTNAME := env_var_or_default('HOSTNAME', '')

# Runs Go tests
test:
    go test ./...

# Checks Go code
vet:
    go vet ./...

# Builds Go example applcations
build:
    go build -o out/ ./...

build-docker:
    docker run -it --rm \
        -v {{justfile_directory()}}/:/build \
        -w /build \
        rstudio/platform-lib:lib-build go build -o out/ ./...

# Cleans Go build directory (out)
clean:
    rm -rf out/

# Build the docker image for building
build-build-env:
    docker build -t rstudio/platform-lib:lib-build -f .github/actions/build/Dockerfile .github/actions/build

# Build the docker image for e2e testing
build-e2e-env:
    docker build --network host -t rstudio/platform-lib:lib-e2e -f .github/actions/test/Dockerfile .github/actions/test

# Creates a container for e2e testing
create-e2e-env:
    #!/usr/bin/env bash
    set -euxo pipefail
    docker inspect platform-lib-e2e -f 'Found existing container' && docker rm platform-lib-e2e || echo "Created container"
    docker create -it --privileged --rm \
        -v {{HOME_DIR}}/.aws:/root/.aws \
        -v /var/run/docker.sock:/var/run/docker.sock \
        -e HOME_DIR={{HOME_DIR}} \
        -e REMOTE_CONTAINERS=${REMOTE_CONTAINERS} \
        -w /test \
        --name platform-lib-e2e rstudio/platform-lib:lib-e2e /bin/bash

# Copies test code to the e2e test container
copy-e2e-env:
    docker cp test/. platform-lib-e2e:/test/
    docker cp out/. platform-lib-e2e:/test/assets/
    docker cp .github/actions/test/assets/.python-version platform-lib-e2e:/test/e2e/

# Start an interactive shell for e2e testing
start-e2e-env: (create-e2e-env) (copy-e2e-env)
    docker start -i platform-lib-e2e

# Stop a running container
stop-e2e-env:
    docker kill platform-lib-e2e

