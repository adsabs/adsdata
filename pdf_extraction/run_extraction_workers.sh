#!/bin/sh

SCRIPT=$(readlink -f $0)
SCRIPTPATH=`dirname $SCRIPT`
BASEPATH=`dirname $SCRIPTPATH`

echo $SCRIPTPATH

$SCRIPTPATH/jython/bin/jython -J-cp "$SCRIPTPATH/lib/*" extract_pdf.py -w 6 "$@"
