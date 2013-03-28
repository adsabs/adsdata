#!/bin/sh

SCRIPT=$(readlink -f $0)
SCRIPTPATH=`dirname $SCRIPT`
BASEPATH=`dirname $SCRIPTPATH`

. $BASEPATH/scripts/sync_mongo_data.py sync
. $BASEPATH/scripts/build_docs.py build
