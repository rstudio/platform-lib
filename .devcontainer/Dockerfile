FROM ubuntu:bionic

ARG USERNAME=rstudio
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN apt-get update && \
    export DEBIAN_FRONTEND=noninteractive && \
    apt-get install -y \
    build-essential \
    clang \
    cmake \
    curl \
    docker.io \
    expect \
    fakeroot \
    gdebi-core \
    git \
    gnupg1 \
    iproute2 \
    libacl1-dev \
    libattr1-dev \
    libbz2-dev \
    libcap-dev \
    libcurl4-openssl-dev \
    libegl1-mesa \
    libfuse2 \
    libgl1-mesa-dev \
    libglib2.0-dev \
    libgtk-3-0 \
    libnss3 \
    libssl-dev \
    libuser1-dev \
    libxml2-dev \
    libxslt1-dev \
    locales \
    lsof \
    ninja-build \
    p7zip-full \
    pkg-config \
    python \
    software-properties-common \
    sudo \
    unzip \
    uuid \
    uuid-dev \
    valgrind \
    vim \
    wget \
    xvfb \
    zlib1g-dev

RUN localedef -i en_US -f UTF-8 en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8
ENV DEBIAN_FRONTEND=noninteractive

# Install AWS CLI v2
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip aws

# Install just
RUN curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- --to /usr/local/bin/

# Install docker-compose
RUN curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
RUN chmod +x /usr/local/bin/docker-compose

# Install Go. Keep in sync with other Dockerfiles.
ENV GOLANG_VERSION 1.17.1
ENV GOLANG_DOWNLOAD_SHA256 dab7d9c34361dc21ec237d584590d72500652e7c909bf082758fb63064fca0ef
RUN curl -fsSL "https://dl.google.com/go/go${GOLANG_VERSION}.linux-amd64.tar.gz" -o golang.tar.gz \
    && echo "$GOLANG_DOWNLOAD_SHA256  golang.tar.gz" | sha256sum -c - \
    && tar -C /usr/local -xzf golang.tar.gz \
    && rm golang.tar.gz

# Install Go tools: gotestcover, shadow, delve
RUN PATH="$PATH:/usr/local/go/bin" GOPATH=/usr/local/gotools go get -v golang.org/x/tools/go/analysis/passes/shadow/cmd/shadow github.com/go-delve/delve/cmd/dlv
ENV PATH="$PATH:/usr/local/gotools/bin:/usr/local/go/bin"

#TODO install these tools:
#   gopkgs
#   go-outline
#   gotests
#   gomodifytags
#   impl
#   goplay
#   dlv-dap
#   staticcheck
#   gopls

# Create the user
RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME \
    && echo "$USERNAME:$USERNAME" | sudo chpasswd
