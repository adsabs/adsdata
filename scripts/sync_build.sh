#!/bin/bash

SCRIPT=$(readlink -f $0)
SCRIPTPATH=`dirname $SCRIPT`
BASEPATH=`dirname $SCRIPTPATH`

# activate our virtualenv
. `dirname $BASEPATH`/python/bin/activate

LOCKFILE="$SCRIPTPATH/.build_sync.lock"

# save PID in lockfile
/proj/ads/soft/bin/mklock -d $SCRIPTPATH .adsdata.lock $$ || {
	echo "$SCRIPT: cannot create lock file, aborting" 1>&2
	exit 1
}

echo "#############" `date` ": synching data sources #################"
python $BASEPATH/scripts/sync_mongo_data.py "$@" sync

echo "#############" `date` ": building docs collection ###################"
python $BASEPATH/scripts/build_docs.py "$@" build -t 3 -i /proj/ads/abstracts/config/bibcodes.list.can

echo "#############" `date` ": processing deletions ###################"
python $BASEPATH/scripts/deletions.py "$@" delete

echo "#############" `date` ": script completed ###################"

/proj/ads/soft/bin/rmlock -d $SCRIPTPATH .adsdata.lock $$

