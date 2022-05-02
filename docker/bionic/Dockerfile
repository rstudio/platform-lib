FROM ubuntu:bionic

RUN apt-get -qq update && \
    export DEBIAN_FRONTEND=noninteractive && \
    apt-get install -qq \
    bash-completion \
    build-essential \
    curl \
    dnsutils \
    docker.io \
    gdebi-core \
    git \
    gnupg1 \
    iproute2 \
    libcurl4-openssl-dev \
    libxml2-dev \
    locales \
    lsof \
    python3 \
    python3-pip \
    python3-software-properties \
    rsync \
    software-properties-common \
    sudo \
    unzip \
    vim \
    wget < /dev/null > /dev/null && \
    rm -rf /var/lib/apt/lists/*

RUN localedef -i en_US -f UTF-8 en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8
ENV DEBIAN_FRONTEND=noninteractive

# Upgrade pip
RUN pip3 install --upgrade pip

# Install AWS CLI v2
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-$(uname -m).zip" -o "awscliv2.zip" && \
    unzip -q awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip aws

# Install just
RUN curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- --to /usr/local/bin/

# Install docker-compose
RUN curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
RUN chmod +x /usr/local/bin/docker-compose

# Install Go. Keep in sync with other Dockerfiles.
ENV GOLANG_VERSION 1.18
RUN ARCH=$(if [ $(uname -m) = "aarch64" ]; then echo "arm64"; else echo "amd64"; fi) \
    && GOLANG_DOWNLOAD_SHA256=$(if [ $(uname -m) = "aarch64" ]; \
      then echo "7ac7b396a691e588c5fb57687759e6c4db84a2a3bbebb0765f4b38e5b1c5b00e"; \
      else echo "e85278e98f57cdb150fe8409e6e5df5343ecb13cebf03a5d5ff12bd55a80264f"; fi) \
    && curl -fsSL "https://dl.google.com/go/go${GOLANG_VERSION}.linux-${ARCH}.tar.gz" -o golang.tar.gz \
    && echo "$GOLANG_DOWNLOAD_SHA256  golang.tar.gz" | sha256sum -c - \
    && tar -C /usr/local -xzf golang.tar.gz \
    && rm golang.tar.gz

# Install Go tools
RUN PATH="$PATH:/usr/local/go/bin" GOPATH=/usr/local/gotools go install \
    golang.org/x/tools/...@latest
RUN PATH="$PATH:/usr/local/go/bin" GOPATH=/usr/local/gotools go install \
    github.com/ramya-rao-a/go-outline@latest
RUN PATH="$PATH:/usr/local/go/bin" GOPATH=/usr/local/gotools go install \
    github.com/fatih/gomodifytags@latest
RUN PATH="$PATH:/usr/local/go/bin" GOPATH=/usr/local/gotools go install \
    github.com/josharian/impl@latest
RUN PATH="$PATH:/usr/local/go/bin" GOPATH=/usr/local/gotools go install \
    github.com/haya14busa/goplay/cmd/goplay@latest
RUN PATH="$PATH:/usr/local/go/bin" GOPATH=/usr/local/gotools go install \
    honnef.co/go/tools/cmd/staticcheck@latest
RUN PATH="$PATH:/usr/local/go/bin" GOPATH=/usr/local/gotools go install \
    github.com/uudashr/gopkgs/v2/cmd/gopkgs@latest
RUN PATH="$PATH:/usr/local/go/bin" go install github.com/go-delve/delve/cmd/dlv@latest
RUN GO111MODULE=on PATH="$PATH:/usr/local/go/bin" GOPATH=/usr/local/gotools go install \
    golang.org/x/tools/gopls@latest
RUN PATH="$PATH:/usr/local/go/bin" GOPATH=/usr/local/gotools go install \
    github.com/cweill/gotests/...
RUN GO111MODULE=on PATH="$PATH:/usr/local/go/bin" GOPATH=/usr/local/gotools GOBIN=/tmp/ go install \
    github.com/go-delve/delve/cmd/dlv@master && \
    mv /tmp/dlv $GOPATH/bin/dlv-dap
ENV PATH="$PATH:/usr/local/gotools/bin:/usr/local/go/bin"
