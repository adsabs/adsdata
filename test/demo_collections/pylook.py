#!/usr/bin/python
#
import sys
import time

def find_key_in_sorted_file(searchkey, fp, fold=False, sepchar='\t', stripkey=False, debug=False):
    """
    Finds and returns a list of records in input file
    which matched the input key in pure python.  Inspired by 
    http://stackoverflow.com/questions/8369175/binary-search-over-a-huge-file-with-unknown-line-length
    """
    begin = 0
    fp.seek(0,2)
    end = fp.tell()

    if fold:
        searchkey = searchkey.lower()
    if debug:
        sys.stderr.write("file length: %s\n" % end)
        sys.stderr.write("search key %s\n" % searchkey)

    found = None
    while found != searchkey and begin < end - 1:
        mid = int((end+begin) / 2)
        fp.seek(mid, 0)
        # realign at beginning of line
        if mid:
            fp.readline()
        cursor = fp.tell()
        record = fp.readline().rstrip().split(sepchar,1)
        if debug:
            sys.stderr.write("begin: %d; end: %d; read record at %d: %s\n" % (begin, end, cursor, record))
            # time.sleep(1)
        found = key = record.pop(0)
        if len(record) > 0:
            value = record.pop()
        else:
            value = ''
        if fold:
            found = found.lower()
        if searchkey == found:
            pass # find what you want
        elif searchkey > found:
            begin = mid
        else:
            end = mid

    results = []
    while found == searchkey:
        if stripkey:
            results.append([ value ])
        else:
            results.append([ key, value ])
        record = fp.readline().rstrip().split(sepchar,1)
        found = key = record.pop(0)
        if len(record) > 0:
            value = record.pop()
        if fold:
            found = found.lower()

    return results


from optparse import OptionParser

if __name__ == "__main__":
    
    op = OptionParser()
    op.set_usage("usage: %s [-d] [-f] [-s] [-t CHAR] key file" % __file__)
    op.add_option('-d','--debug', dest="debug", action="store_true", default=False)
    op.add_option('-f','--fold', dest="fold", action="store_true", default=False)
    op.add_option('-s','--stripkey', dest="stripkey", action="store_true", default=False)
    op.add_option('-t','--separator', dest="separator", action="store", default='\t')
    opts, args = op.parse_args()

    try:
        searchkey = args.pop(0)
    except IndexError:
        op.error("missing key argument")
    try:
        file = args.pop(0)
    except IndexError:
        op.error("missing file argument")
    try:
        fp = open(file)
    except IOError:
        op.error("cannot open file %s" % file)

    results = find_key_in_sorted_file(searchkey, fp, 
                                      fold=opts.fold, 
                                      sepchar=opts.separator, 
                                      stripkey=opts.stripkey,
                                      debug=opts.debug)
    
    for r in results:
        print opts.separator.join([ str(k) for k in r ])


