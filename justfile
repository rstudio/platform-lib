export REMOTE_CONTAINERS := env_var_or_default('REMOTE_CONTAINERS', 'false')

# When running in VS Code in a docker container, this is set to the home directory
# of the host machine. Since bind mount `/var/run/docker.sock` from the host to the
# VSCode docker container, we can kick off additional docker processes from VSCode,
# but any directories we mount need to know the host's home directory.
HOME_DIR := env_var_or_default('HOST_HOME', env_var('HOME'))

HOSTNAME := env_var_or_default('HOSTNAME', env_var('HOSTNAME'))

# Runs Go tests
test:
    go test ./...

# Checks Go code
vet:
    go vet ./...

# Builds Go example applcations
build:
    go build -o out/ ./...

# Cleans Go build directory (out)
clean:
    rm -rf out/

# Ensure we have a Dockerfile for the specified distro
ensure-dockerfile distro:
    test -f test/docker/{{distro}}/Dockerfile || (echo "Could not locate Dockerfile in test/docker/{{distro}}" && exit 1)

# Build the specified distro docker image for e2e testing
build-e2e-env distro: (ensure-dockerfile distro)
    docker build --network host --build-arg TEST_ENV_NAME={{distro}} -t rstudio/platform-lib:lib-e2e-{{distro}} -f test/docker/{{distro}}/Dockerfile .

# Creates a container for e2e testing
create-e2e-env distro:
    #!/usr/bin/env bash
    set -euxo pipefail
    docker inspect platform-lib-e2e-{{distro}} -f 'Found existing container' && docker rm platform-lib-e2e-{{distro}} || echo "Created container"
    docker create -it --privileged --rm \
        -v {{HOME_DIR}}/.aws:/root/.aws \
        -v /var/run/docker.sock:/var/run/docker.sock \
        -e HOME_DIR={{HOME_DIR}} \
        -e REMOTE_CONTAINERS=${REMOTE_CONTAINERS} \
        -e TEST_ENV_NAME={{distro}} \
        --name platform-lib-e2e-{{distro}} rstudio/platform-lib:lib-e2e-{{distro}} /bin/bash

# Copies test code to the e2e test container
copy-e2e-env distro:
    docker cp test/. platform-lib-e2e-{{distro}}:/test/
    docker cp out/. platform-lib-e2e-{{distro}}:/test/assets/

# Start an interactive shell for e2e testing in the specified distro
start-e2e-env distro: (ensure-dockerfile distro) (create-e2e-env distro) (copy-e2e-env distro)
    docker start -i platform-lib-e2e-{{distro}}

# Stop a running container using the specified distro
stop-e2e-env distro:
    docker kill platform-lib-e2e-{{distro}}

