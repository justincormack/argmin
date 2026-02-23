#!/bin/sh

# Keep a checkout of dependencies we use for reference
# currently isa-l for erasure coding

PROJECT_ROOT="$(git rev-parse --show-toplevel 2>/dev/null)" || {
  echo "Not inside a git repo"
  exit 1
}

mkdir -p ${PROJECT_ROOT}/tmp

cd ${PROJECT_ROOT}/tmp && {
  [ -d 'isa-l' ] && [ ! -d 'isa-l/.git '] && rm -rf 'isa-l'
  if [ ! -d 'isa-l/.git' ]
  then
    git clone https://github.com/intel/isa-l.git
  else
    cd 'isa-l' && git pull
  fi
}
