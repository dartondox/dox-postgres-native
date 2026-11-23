#!/usr/bin/env bash
# Checks a ready-built Linux library before it goes into a release.
#
#   tool/check_linux_lib.sh <library> <glibc floor, eg. 2.34>
#
# 1. The library must not need libgcc_s. A `FROM scratch` Docker image with
#    only the Dart `/runtime/` files does not have it.
# 2. The library must not need a glibc newer than the floor.
set -euo pipefail

lib=$1
floor=$2

needed=$(readelf -d "$lib" | grep NEEDED)
echo "$needed"
if grep -q libgcc_s <<<"$needed"; then
  echo "::error::$lib needs libgcc_s"
  exit 1
fi

newest=$(readelf --dyn-syms -W "$lib" | grep -o 'GLIBC_[0-9.]*' | sort -uV | tail -n 1)
echo "newest glibc symbol: $newest (floor: $floor)"
if [ "$(printf '%s\n' "GLIBC_$floor" "$newest" | sort -V | tail -n 1)" != "GLIBC_$floor" ]; then
  echo "::error::$lib needs $newest, the floor is $floor"
  exit 1
fi
