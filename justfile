export REMOTE_CONTAINERS := env_var_or_default('REMOTE_CONTAINERS', 'false')

# When running in VS Code in a docker container, this is set to the home directory
# of the host machine. Since bind mount `/var/run/docker.sock` from the host to the
# VSCode docker container, we can kick off additional docker processes from VSCode,
# but any directories we mount need to know the host's home directory.
HOME_DIR := env_var_or_default('HOST_HOME', env_var('HOME'))

HOSTNAME := env_var_or_default('HOSTNAME', '')

# Runs Go unit tests
test:
    go test ./...

# Checks Go code
vet:
    go vet ./...

# Builds Go code natively.
build:
    go build -o out/ ./...

# Builds Go code using docker. Useful when using a MacOS or Windows native IDE. First,
# run `just build-build-env' to create the docker image you'll need.
build-docker:
    docker run -it --rm \
        -v {{justfile_directory()}}/:/build \
        -w /build \
        rstudio/platform-lib:lib-build go build -o out/ ./...

# Cleans Go build directory (out)
clean:
    rm -rf out/

# Builds the docker image used for building Go code
build-build-env:
    docker build -t rstudio/platform-lib:lib-build -f .github/actions/build/Dockerfile .github/actions/build

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
