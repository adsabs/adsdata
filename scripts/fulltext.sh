#!/bin/sh

SCRIPT=$(readlink -f $0)
SCRIPTPATH=`dirname $SCRIPT`
BASEPATH=`dirname $SCRIPTPATH`

# get ADS environment
[ "x$ADS_ENVIRONMENT" = "x" ] && eval `$HOME/.adsrc sh`

# activate our virtualenv
. `dirname $BASEPATH`/python/bin/activate

LOCKFILE="$SCRIPTPATH/.build_sync.lock"

# save PID in lockfile
/proj/ads/soft/bin/mklock -d $SCRIPTPATH .fulltext.lock $$ || {
	echo "$SCRIPT: cannot create lock file, aborting" 1>&2
	exit 1
}

#echo "#############" `date` ": starting pdf extraction workers ###################"
#supervisorctl start adsdata-pdf-extract

echo "#############" `date` ": extracting fulltext ###################"
python $BASEPATH/scripts/extract_fulltext.py "$@" -i "$ADS_ABSCONFIG/links/fulltext/all.links" extract

#echo "#############" `date` ": stopping pdf extraction workers ###################"
#supervisorctl stop adsdata-pdf-extract

/proj/ads/soft/bin/rmlock -d $SCRIPTPATH .fulltext.lock $$

