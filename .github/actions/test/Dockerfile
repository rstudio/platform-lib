FROM ubuntu:bionic

ARG TEST_ENV_NAME

RUN echo "export LICENSING_DISTRO="$TEST_ENV_NAME>> /etc/environment

RUN useradd -m rstudiotest

RUN apt-get -qq update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -qq \
    curl \
    docker.io \
    git \
    jq \
    libbz2-dev \
    libffi-dev \
    liblzma-dev \
    libreadline-dev \
    libcurl4-openssl-dev \
    libncurses5-dev \
    libssl-dev \
    libsqlite3-dev \
    libxml2-dev \
    libxmlsec1-dev \
    python3-bz2file \
    python3-pip \
    python3-venv \
    sudo \
    vim \
    unzip \
    zlib1g-dev < /dev/null > /dev/null && \
    rm -rf /var/lib/apt/lists/*

ARG JENKINS_GID=999
ARG JENKINS_UID=999
RUN groupadd -g $JENKINS_GID jenkins && \
    useradd -m -d /var/lib/jenkins -u $JENKINS_UID -g jenkins jenkins && \
    echo "jenkins ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers && \
    echo "Defaults        secure_path='/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin:/usr/local/pyenv/bin'" >> /etc/sudoers

# AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip -q awscliv2.zip && \
    ./aws/install && \
    rm awscliv2.zip

# Pyenv
RUN git clone https://github.com/pyenv/pyenv.git /usr/local/pyenv && \
    cd /usr/local/pyenv && \
    git checkout v1.2.23 && \
    src/configure && make -C src && \
    cd - && \
    git clone https://github.com/pyenv/pyenv-virtualenv.git /usr/local/pyenv/plugins/pyenv-virtualenv && \
    cd /usr/local/pyenv/plugins/pyenv-virtualenv && \
    git checkout v1.1.5 && \
    cd -

ENV PYENV_ROOT="/usr/local/pyenv"
ENV PATH="/usr/local/pyenv/bin:${PATH}"
RUN echo "export PYENV_ROOT=/usr/local/pyenv" >> /etc/environment && \
    echo "export PATH=$PATH" >> /etc/environment

# Install BATS test framework.
RUN git clone https://github.com/bats-core/bats-core.git && \
    cd bats-core && \
    ./install.sh /usr/local && \
    cd ../ && \
    rm -rf bats-core

# Install just
RUN curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- --to /usr/local/bin/

# Install PyEnv
RUN mkdir /pyenv
COPY assets/.python-version /pyenv/.python-version
COPY assets/requirements.txt /pyenv/requirements.txt
RUN cd /pyenv && ls -lha && pyenv install && \
    eval "$(pyenv init -)" && \
    eval "$(pyenv virtualenv-init -)" && \
    pip install -r requirements.txt

COPY assets/test.sh /test.sh
RUN chmod 755 /test.sh
