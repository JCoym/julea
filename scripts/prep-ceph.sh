#!/bin/bash

BLUESTORE_DIR=$1
BLKDEV=$2

if [ ! -d "$BLUESTORE_DIR" ]; then
    mkdir "$BLUESTORE_DIR"
fi
if [ "$(ls -A $DIR)" ]; then
    rm -rf "$BLUESTORE_DIR/*"
fi
ln -s "$BLKDEV" "$BLUESTORE_DIR/block"
