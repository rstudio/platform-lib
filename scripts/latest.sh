#!/usr/bin/env bash

# Refresh tags
git fetch --tags

mods=$(find ./pkg/ -name go.mod | sed -e 's/^\.\///' | sed -e 's/\/go\.mod//')

function versions_for_module {
  mod=$1
  vers=$(git tag -l | grep $mod)
  for ver in $vers; do
    modonly=$(echo $ver | sed 's/\(.*\)\/.*/\1/')
    if [[ $modonly == $mod ]]; then
      veronly=$(echo $ver | rev | cut -d/ -f1 | rev)
      echo $modonly $veronly
    fi
  done
}

for mod in $mods; do
  versions_for_module $mod | sort | tail -1
done
