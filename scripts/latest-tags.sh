#!/usr/bin/env bash

# Refresh tags
git fetch --tags

# Find all `go.mod` files and trim the leading "./" and the trailing "/go.mod".
# This leaves us with the package name, like "pkg/rsqueue/impls/database".
mods=$(find ./pkg/ -name go.mod | sed -e 's/^\.\///' | sed -e 's/\/go\.mod//')

function versions_for_module {
  mod=$1

  # Get tags matching the module
  vers=$(git tag -l | grep $mod)

  # For each version in the module
  for ver in $vers; do
    # Trim off everything after and including the last slash.
    modonly=$(echo $ver | sed 's/\(.*\)\/.*/\1/')

    # If the trimmed module matches the module, then get the versions.
    # This prevents false positive matches with submodules. For example,
    # "pkg/rsnotify" will not match "pkg/rsnotify/listeners/local".
    if [[ $modonly == $mod ]]; then

      # The version is everything after the last "/".
      veronly=$(echo $ver | rev | cut -d/ -f1 | rev)
      echo $modonly $veronly
    fi
  done
}

# For each module, find the latest version
for mod in $mods; do
  # List all versions for the module, sort, and print the last one.
  versions_for_module $mod | sort | tail -1
done
