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

echo "#############" `date` ": synching fulltext links source #################"
python $BASEPATH/scripts/sync_mongo_data.py --collection fulltext_links "$@" sync

echo "#############" `date` ": starting pdf extraction workers ###################"
supervisorctl start adsdata-pdf-extract

echo "#############" `date` ": extracting fulltext ###################"
python $BASEPATH/scripts/extract_fulltext.py "$@" extract

echo "#############" `date` ": stopping pdf extraction workers ###################"
supervisorctl stop adsdata-pdf-extract

/proj/ads/soft/bin/rmlock -d $SCRIPTPATH .adsdata.lock $$

