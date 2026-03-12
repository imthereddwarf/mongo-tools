import readers
import sys
import json
import collections
import sys, os
import datetime
import pprint
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from argparse import ArgumentTypeError


__all__ = []
__version__ = 0.1
__date__ = '2025-01-17'
__updated__ = '2025-01-17'


def valid_date(s):
    try:
        if len(s) < 11:
            return datetime.datetime.strptime(s, "%Y-%m-%d")
        elif len(s) <=19:
            return datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")
        else:
            return datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f%z")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise ArgumentTypeError(msg)
    
    
program_name = os.path.basename(sys.argv[0])
program_version = "v%s" % __version__
program_build_date = str(__updated__)
program_version_message = '%%(prog)s %s (%s)' % (program_version, program_build_date)
program_shortdesc = "Grep ftdc"
program_license = '''%s
  Created by Pete Williamson on %s.
  Copyright 2020 MongoDB Inc . All rights reserved.

  Licensed under the Apache License 2.0
  http://www.apache.org/licenses/LICENSE-2.0

  Distributed on an "AS IS" basis without warranties
  or conditions of any kind, either express or implied.

USAGE
''' % (program_shortdesc, str(__date__))

# Setup argument parser
parser = ArgumentParser(description=program_license, formatter_class=RawDescriptionHelpFormatter)
parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", help="verbose, include Informational messages")
parser.add_argument("--debug", dest="debug", action="store_true", help="debug")
parser.add_argument('--max',  dest="key", metavar='key path', help="FTDC keyname to search for")
parser.add_argument('--failurefile', '-ff', dest="failFile", metavar='failFile', help="Save errors to a file")
parser.add_argument("--startdate", dest="start", metavar="Start From", help="The Start Date - format YYYY-MM-DD", type=valid_date)
parser.add_argument("--enddate", dest="end", metavar="Start From", help="The Start Date - format YYYY-MM-DD", type=valid_date)
parser.add_argument('-V', '--version', action='version', version=program_version_message)
parser.add_argument('--meta', dest="showMeta", action="store_true", help="Input file is UTF-8 rather than ISO-8859-1")
parser.add_argument('--namePattern', dest="namePat", metavar='pattern', help="Regex to match the short name")
parser.add_argument('--wrapper', dest="wrapper", metavar='wrapper', help="Regex to strip any wrapper")
parser.add_argument(dest="paths", help="paths to folder(s) with source file(s) [default: %(default)s]", metavar="path", nargs='+')

# Process arguments
args = parser.parse_args()

if args.key != None:
    searchKey = tuple(args.key.split())
else:
    searchKey = None
    
tskey = tuple(['start'])

for path in args.paths:
    ftdc = path
    
    if args.showMeta:
        for meta in readers.read_meta(ftdc):
            pprint.pprint(meta, sort_dicts=False)
            exit()
    else:
        if searchKey == None:
            for chunk in readers.read_ftdc(ftdc,True):
                for key in chunk.keys():
                    print(key)
                    # data = chunk[key]
                exit()
        else:
            maxval = 0
            maxtime = 0
            for chunk in readers.read_ftdc(ftdc,True):
                index = 0
                try: 
                    lastval = chunk[searchKey][0]
                    for data in chunk[searchKey]:
                        inc = data - lastval
                        if inc >= maxval and inc > 0:
                            maxval = inc
                            millis = chunk[tskey][index]
                            maxtime = datetime.datetime.utcfromtimestamp(int(millis/1000))
                            print(maxval,maxtime)
                        index += 1
                        lastval = data
                except KeyError:
                    pass
            print(maxval, maxtime)
        
