#!/bin/bash
cd test && \
    cp /pyenv/.python-version ${INPUT_WORKSPACE}/test/e2e/.python-version && \
    eval "$(pyenv init -)" && \
    eval "$(pyenv virtualenv-init -)" && \
    TEST_ENV_NAME=bionic ASSET_DIR=${INPUT_WORKSPACE}/out just test && \
    cd -
