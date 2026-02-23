#!/bin/sh

# Keep a checkout of my Obsidian notes, just the relevant parts for reference

PROJECT_ROOT="$(git rev-parse --show-toplevel 2>/dev/null)" || {
  echo "Not inside a git repo"
  exit 1
}

cd ${PROJECT_ROOT} && {
  [ -d notes ] && [ ! -d notes/.git ] && rm -rf notes
  if [ ! -d notes/.git ]
  then
    git clone --no-checkout git@github.com:justincormack/obsidian.git notes && \
    cd notes && git sparse-checkout init --no-cone && \
    git sparse-checkout set cloud/ && \
    git checkout -f HEAD
  else
    cd notes/ && git pull
  fi
}
