#!/bin/sh

SCRIPT=$(readlink -f $0)
SCRIPTPATH=`dirname $SCRIPT`
BASEPATH=`dirname $SCRIPTPATH`

# activate our virtualenv
. `dirname $BASEPATH`/python/bin/activate

FROM_MONGO="mongodb://adszee:27017/solr4ads"
LOCKFILE="$SCRIPTPATH/.build_sync.lock"

if [ -f $LOCKFILE ]
then
    echo "Lock file exists. Aborting."
    exit 1
else
    echo "Creating lockfile"
    touch $LOCKFILE
fi

echo "#############" `date` ": synching data sources #################"
python $BASEPATH/scripts/sync_mongo_data.py "$@" sync

echo "#############" `date` ": copying fulltext ###################"
python $BASEPATH/scripts/copy_fulltext.py --from_mongo=$FROM_MONGO "$@"

echo "#############" `date` ": building docs collection ###################"
python $BASEPATH/scripts/build_docs.py "$@" build

echo "#############" `date` ": processing deletions ###################"
python $BASEPATH/scripts/deletions.py "$@" delete

echo "Removing lockfile"
rm -f $LOCKFILE

