#!/usr/bin/python
"""
@author: aaccomazzi
"""

import json
import os
import sys
import site
site.addsitedir(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from adsdata import utils

# should be in some systemwide package
from pylook import find_key_in_sorted_file

def get_bibcodes_from_json(file):
    """
    reads a list of json structures from an input file, 
    concatenates them, and returns the list of 
    corresponding bibcodes (Note: bibcodes must be in
    the '_id' field)
    """
    obj = []
    with open(file) as f:
        # the contents of these files are lists of json records,
        # so we need to concatenate them into an array
        s = '[ ' + ', '.join(f.readlines()) + ' ]'
        try:
            obj = json.loads(s)
        except ValueError:
            obj = []
    return map(lambda x: x.get('_id'), obj)

def bibcode_lookup(file, biblist):
    """
    takes a list of bibcodes, looks them up in a 
    case-insensitive sorted file, and returns the list
    of records found
    """
    records = []
    with open(file) as f:
        for b in biblist:
            sys.stderr.write("searching for %s in file %s\n" % (b, file))
            res = find_key_in_sorted_file(b, f, fold=True)
            for r in res:
                records.append("\t".join([ str(k) for k in r ]))
    return records

if __name__ == "__main__":

    demo_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'demo_data')
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    config = utils.load_config(os.path.join(base_dir, 'adsdata.cfg'))

    for f in os.listdir(demo_dir):
        abs_path = os.path.join(demo_dir, f)
        cname = os.path.splitext(f)[0]
        cfile = config.get('collections',{}).get(cname)
        if not cfile:
            sys.stderr.write("no file found for collection %s, skipped\n" % cname)
            continue
        # read bibcodes from local collection, look them up in global file
        bibcodes = get_bibcodes_from_json(abs_path)
        sys.stderr.write("read %d bibcodes from file %s\n" % (len(bibcodes), abs_path))
#        sys.stderr.write("first record: %s\n" % str(bibcodes[0]))
        records = bibcode_lookup(cfile, bibcodes)
        if not len(records):
            sys.stderr.write("no records found in file %s\n" % cfile)
            continue
        sys.stderr.write("found %d records in file %s\n" % (len(records), cfile))
        tfile = cname + '.tsv'
        with open(tfile, "w") as f:
            f.writelines(map(lambda x: str(x) + "\n", records))
        sys.stderr.write("written %d records to file %s\n" % (len(records), tfile))

            
