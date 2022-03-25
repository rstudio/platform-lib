#!/usr/bin/env bash

# Constants
MODDIR=".chart"

# Install `gomod` if needed.
if ! which gomod; then
  go install github.com/Helcaraxan/gomod@v0.6.2
fi

(
  cd $MODDIR

  # Generate graph data.
  rm -f graph.dot.1
  gomod graph -a "rdeps(github.com/rstudio/platform-lib/pkg/...)" -o graph.dot.1

  # grep: Remove references to the dummy "chart" package.
  # sed: Remove the "github.com/rstudio/platform-lib/pkg/" prefix.
  < graph.dot.1 \
    grep -v github.com/rstudio/platform-lib/chart | \
    sed -e 's/github.com\/rstudio\/platform-lib\/pkg\///g' \
  > graph.dot
)

# graphviz required
if ! which dot; then
  echo "please install graphviz"
  exit 1
fi

# Generate graph
dot -Tsvg -Kcirco -ograph.svg $MODDIR/graph.dot
