#!/bin/sh

SCRIPT=$(readlink -f $0)
SCRIPTPATH=`dirname $SCRIPT`
BASEPATH=`dirname $SCRIPTPATH`

python $BASEPATH/scripts/sync_mongo_data.py "$@" sync
python $BASEPATH/scripts/build_docs.py "$@" build
python $BASEPATH/scripts/deletions.py "$@" delete
