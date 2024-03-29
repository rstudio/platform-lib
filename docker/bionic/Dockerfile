FROM ubuntu:bionic

ARG TARGETARCH

RUN if [ "${TARGETARCH}" != "amd64" ] && [ "${TARGETARCH}" != "arm64" ]; then \
    echo "${TARGETARCH} is not a supported architecture. Please use linux/amd64 or linux/arm64"; \
    exit 1; fi

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
ENV GOLANG_VERSION 1.20.4
RUN GOLANG_DOWNLOAD_SHA256=$(if [ "${TARGETARCH}" = "arm64" ]; \
      then echo "105889992ee4b1d40c7c108555222ca70ae43fccb42e20fbf1eebb822f5e72c6"; \
      else echo "698ef3243972a51ddb4028e4a1ac63dc6d60821bf18e59a807e051fee0a385bd"; fi) \
    && curl -fsSL "https://dl.google.com/go/go${GOLANG_VERSION}.linux-${TARGETARCH}.tar.gz" -o golang.tar.gz \
    && echo "${GOLANG_DOWNLOAD_SHA256}  golang.tar.gz" | sha256sum -c - \
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
RUN PATH="$PATH:/usr/local/go/bin" GOPATH=/usr/local/gotools go install \
    golang.org/x/tools/gopls@latest
RUN PATH="$PATH:/usr/local/go/bin" GOPATH=/usr/local/gotools go install \
    github.com/cweill/gotests/...
RUN PATH="$PATH:/usr/local/go/bin" GOPATH=/usr/local/gotools GOBIN=/tmp/ go install \
    github.com/go-delve/delve/cmd/dlv@master && \
    mv /tmp/dlv $GOPATH/bin/dlv-dap
ENV PATH="$PATH:/usr/local/gotools/bin:/usr/local/go/bin"
