#!/bin/bash -xe
# Copyright (c) 2014, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.
#
# Tests that the Kudu client sample code can be built out-of-tree and runs
# properly.

ROOT=$(readlink -f $(dirname "$BASH_SOURCE")/../../..)
NUM_PROCS=$(cat /proc/cpuinfo | grep processor | wc -l)

# Install the client library to a temporary directory.
# Try to detect whether we're building using Ninja or Make.
LIBRARY_DIR=$(mktemp -d)
PREFIX_DIR=$LIBRARY_DIR/usr/local
SAMPLES_DIR=$PREFIX_DIR/share/doc/kuduClient/samples
pushd $ROOT
NINJA=$(which ninja 2>/dev/null) || NINJA=""
if [ -r build.ninja -a -n "$NINJA" ]; then
  DESTDIR=$LIBRARY_DIR ninja -j$NUM_PROCS install
else
  make -j$NUM_PROCS DESTDIR=$LIBRARY_DIR install
fi
popd

# Build the client samples using the client library.
# We can just always use Make here, since we're calling cmake ourselves.
pushd $SAMPLES_DIR
CMAKE_PREFIX_PATH=$PREFIX_DIR cmake .
make -j$NUM_PROCS
popd

# Start master+ts
export TEST_TMPDIR=${TEST_TMPDIR:-/tmp/kudutest-$UID}
mkdir -p $TEST_TMPDIR
BASE_DIR=$(mktemp -d --tmpdir=$TEST_TMPDIR client_samples-test.XXXXXXXX)
$ROOT/build/latest/kudu-master \
  --log_dir=$BASE_DIR \
  --fs_wal_dir=$BASE_DIR/master \
  --fs_data_dirs=$BASE_DIR/master &
MASTER_PID=$!
$ROOT/build/latest/kudu-tserver \
  --log_dir=$BASE_DIR \
  --fs_wal_dir=$BASE_DIR/ts \
  --fs_data_dirs=$BASE_DIR/ts &
TS_PID=$!

# Let them run for a bit.
sleep 5

# Run the samples (temporarily disabling exit-on-error).
set +e
$SAMPLES_DIR/sample
RESULT=$?
set -e

# Clean up.
kill -9 $TS_PID || :
kill -9 $MASTER_PID || :
rm -rf $BASE_DIR
rm -rf $LIBRARY_DIR

if [ $RESULT != 0 ]; then
  echo "Sample test failed!"
  exit $RESULT
fi