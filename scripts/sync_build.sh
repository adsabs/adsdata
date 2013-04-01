#!/bin/sh

SCRIPT=$(readlink -f $0)
SCRIPTPATH=`dirname $SCRIPT`
BASEPATH=`dirname $SCRIPTPATH`

echo "#############" `date` ": synching data sources #################"
python $BASEPATH/scripts/sync_mongo_data.py "$@" sync

echo "#############" `date` ": synching data files ###################"
python $BASEPATH/scripts/copy_fulltext.py --from_mongo=mongodb://adszee:27017/solr4ads "$@"

echo "#############" `date` ": synching data files ###################"
python $BASEPATH/scripts/build_docs.py "$@" build

echo "#############" `date` ": synching data files ###################"
python $BASEPATH/scripts/deletions.py "$@" delete
