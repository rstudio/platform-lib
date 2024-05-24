#!/usr/bin/env bash

# Constants
MODDIR=".chart"
MODFILE="${MODDIR}/go.mod"
GODIR="${MODDIR}/cmd/chart"
GOFILE="$GODIR/main.go"
GOFILETEST="$GODIR/main_test.go"
GOVER='1.17'
COPYRIGHT='// Copyright (C) 2022 by RStudio, PBC.'

# Enumerate module latest versions
mods=$(./scripts/latest-tags.sh)

# Set the field separator to new line
IFS=$'\n'

# Generates a list of required platform-lib dependencies
# for the go.mod.
function requires {
  for mod in ${mods}; do
    echo "    github.com/rstudio/platform-lib/${mod}"
  done
}

# Build the go.mod file
function generate_go_mod {
cat <<EOT > ${MODFILE}
module github.com/rstudio/platform-lib/chart

go $GOVER

require (
$(requires)
)
EOT
}

# Generates a list of imports for main.go
function imports {
  (
  cd $MODDIR || exit
  for mod in ${mods}; do
    # Get the module and version
    ver=$(echo "${mod}" | cut -d' ' -f2)
    mod=$(echo "${mod}" | cut -d' ' -f1)

    # Populate Go cache
    go get "github.com/rstudio/platform-lib/${mod}@$ver"

    # Generate import. We have to use `go list` to list importable
    # packages in the module since some modules have nothing in
    # the root directory. For example,
    # "github.com/rstudio/platform-lib/v2/pkg/rsqueue" cannot be imported,
    # but "github.com/rstudio/platform-lib/v2/pkg/rsqueue/agent" can be.
    import=$(go list "github.com/rstudio/platform-lib/${mod}/..." | head -1)
    # Occasionally, we'll end up with an empty/blank import. I'm not sure
    # why, but it seems safe to ignore it.
    if [[ "$import" != "" ]]; then
      echo "    _ \"$import\""
    fi
  done
  )
}

# Build the main.go file
function generate_main {
cat <<EOT > ${GOFILE}
package main

${COPYRIGHT}

import (
$(imports | sort)
)

func main() {}
EOT

cat <<EOT > ${GOFILETEST}
package main

${COPYRIGHT}

import (
	"testing"

	"gopkg.in/check.v1"
)

func TestPackage(t *testing.T) { check.TestingT(t) }
EOT
}

# Create required directories
mkdir -p ${GODIR}

# Generate go.mod and cmd/chart/main.go
generate_go_mod
generate_main

# Format go file
go fmt ${GOFILE}

# Tidy modules
(
  cd ${MODDIR} || exit
  go mod tidy
)
