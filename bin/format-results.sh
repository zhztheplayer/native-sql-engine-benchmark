#!/bin/bash

if [ $# -lt 1 ]; then
  echo "Usage: format-results <input file>" 1>&2
  exit 1
fi

FILE_LOCATION=$1

cat $FILE_LOCATION | sed -nE '/^q[0-9]+/p' | sed -E 's/\s+/\t/g'
