FROM golang:1.22

# Install just
RUN curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- --to /usr/local/bin/

# Install Go tools
#RUN PATH="$PATH:/usr/local/go/bin" GOPATH=/usr/local/gotools go install \
#    golang.org/x/tools/...@latest
#RUN PATH="$PATH:/usr/local/go/bin" GOPATH=/usr/local/gotools go install \
#    github.com/ramya-rao-a/go-outline@latest
#RUN PATH="$PATH:/usr/local/go/bin" GOPATH=/usr/local/gotools go install \
#    github.com/fatih/gomodifytags@latest
#RUN PATH="$PATH:/usr/local/go/bin" GOPATH=/usr/local/gotools go install \
#    github.com/josharian/impl@latest
#RUN PATH="$PATH:/usr/local/go/bin" GOPATH=/usr/local/gotools go install \
#    github.com/haya14busa/goplay/cmd/goplay@latest
#RUN PATH="$PATH:/usr/local/go/bin" GOPATH=/usr/local/gotools go install \
#    honnef.co/go/tools/cmd/staticcheck@latest
#RUN PATH="$PATH:/usr/local/go/bin" GOPATH=/usr/local/gotools go install \
#    github.com/uudashr/gopkgs/v2/cmd/gopkgs@latest
#RUN PATH="$PATH:/usr/local/go/bin" go install github.com/go-delve/delve/cmd/dlv@latest
#RUN GO111MODULE=on PATH="$PATH:/usr/local/go/bin" GOPATH=/usr/local/gotools go install \
#    golang.org/x/tools/gopls@latest
#RUN PATH="$PATH:/usr/local/go/bin" GOPATH=/usr/local/gotools go install \
#    github.com/cweill/gotests/...
#RUN PATH="$PATH:/usr/local/go/bin" GOPATH=/usr/local/gotools GOBIN=/tmp/ go install \
#    github.com/go-delve/delve/cmd/dlv@master && \
#    mv /tmp/dlv $GOPATH/bin/dlv-dap
#ENV PATH="$PATH:/usr/local/gotools/bin:/usr/local/go/bin"
