#!/bin/sh

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

#echo "#############" `date` ": extracting fulltext ###################"
#python $BASEPATH/scripts/extract_fulltext.py "$@" extract

echo "#############" `date` ": building docs collection ###################"
python $BASEPATH/scripts/build_docs.py "$@" build

echo "#############" `date` ": processing deletions ###################"
python $BASEPATH/scripts/deletions.py "$@" delete

/proj/ads/soft/bin/rmlock -d $SCRIPTPATH .adsdata.lock $$

