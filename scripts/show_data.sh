#!/bin/sh

HEADER="triggers,window,is_first,is_last,timing,n_before,n_after,N"

LAST_FILENAME=$(ls -t "$1/" | head -1)

echo $HEADER
cat $1/$LAST_FILENAME