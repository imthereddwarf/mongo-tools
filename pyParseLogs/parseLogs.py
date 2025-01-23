'''
XmlImport.pyXmlImport -- Import XMl into MongoDB

XmlImport.pyXmlImport is a description

It defines classes_and_methods

@author:     user_name

@copyright:  2020 organization_name. All rights reserved.

@license:    license

@contact:    user_email
@deffield    updated: Updated
'''

import pymongo
from bson import json_util
from bson.objectid import ObjectId
from decimal import Decimal
from bson.decimal128 import Decimal128
import json, re
import simplejson
import datetime
import sys, os
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from argparse import ArgumentTypeError
import traceback
import pytz
from platform import _ver_stages
import importJson
from myLogger import myLogger
import importText






__all__ = []
__version__ = 0.1
__date__ = '2025-01-17'
__updated__ = '2025-01-17'



jsonTypes = []
keyTypes = {}
cmdTypes = {}
strTypes = {}
logger = None


myClient = None
mydb = None
mycol = None


def fixDollar(indoc):
    newdoc = {}
    for keyName in indoc:
        if keyName.startswith("$"):
            newKey = "_"+keyName
        else:
            newKey = keyName
        if isinstance(indoc[keyName],dict):
            newdoc[newKey] = fixDollar(indoc[keyName])
        elif isinstance(indoc[keyName],list):
            newArray = []
            for item in indoc[keyName]:
                if isinstance(item,dict):
                    newArray.append(fixDollar(item))
                else:
                    newArray.append(item)
            newdoc[newKey] = newArray
        else:
            newdoc[newKey] = indoc[keyName]
    #print(indoc)
    #print(newdoc)
    return newdoc


    


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
        
   
def main(argv=None):
    
    global logger
    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)
    linecount = 0
    lineBuffer = ""
    program_name = os.path.basename(sys.argv[0])
    program_version = "v%s" % __version__
    program_build_date = str(__updated__)
    program_version_message = '%%(prog)s %s (%s)' % (program_version, program_build_date)
    program_shortdesc = __import__('__main__').__doc__.split("\n")[1]
    program_license = '''%s

  Created by Pete Williamson on %s.
  Copyright 2020 MongoDB Inc . All rights reserved.

  Licensed under the Apache License 2.0
  http://www.apache.org/licenses/LICENSE-2.0

  Distributed on an "AS IS" basis without warranties
  or conditions of any kind, either express or implied.

USAGE
''' % (program_shortdesc, str(__date__))
    try:
        # Setup argument parser
        parser = ArgumentParser(description=program_license, formatter_class=RawDescriptionHelpFormatter)
        parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", help="verbose, include Informational messages")
        parser.add_argument("--debug", dest="debug", action="store_true", help="debug")
        parser.add_argument('--logfile', '-l', dest="logFile", metavar='logFile', help="Redirect Messages to a file")
        parser.add_argument('--failurefile', '-ff', dest="failFile", metavar='failFile', help="Save errors to a file")
        parser.add_argument("--startdate", dest="start", metavar="Start From", help="The Start Date - format YYYY-MM-DD", type=valid_date)
        parser.add_argument("--enddate", dest="end", metavar="Start From", help="The Start Date - format YYYY-MM-DD", type=valid_date)
        parser.add_argument('-V', '--version', action='version', version=program_version_message)
        parser.add_argument('--utf', dest="utfEncoding", action="store_true", help="Input file is UTF-8 rather than ISO-8859-1")
        parser.add_argument('--URI', dest="URI", metavar='uri', help="MongoDb database URI")
        parser.add_argument('-c', '--coll', dest="coll", metavar='coll', help="Collection to import into")
        parser.add_argument('--namePattern', dest="namePat", metavar='pattern', help="Regex to match the short name")
        parser.add_argument('--wrapper', dest="wrapper", metavar='wrapper', help="Regex to strip any wrapper")
        parser.add_argument(dest="paths", help="paths to folder(s) with source file(s) [default: %(default)s]", metavar="path", nargs='+')

        # Process arguments
        args = parser.parse_args()
        #print(args.URI)
        paths = args.paths
        procrange = {}
        wrapper = None

            
        if args.debug:
            logLevel = myLogger.DEBUG
        elif args.verbose:
            logLevel = myLogger.INFO
        else:
            logLevel = myLogger.MESSAGE
        
        logger = myLogger("logParser",file=args.logFile,datafile=args.failFile,severity=logLevel)

        if args.start != None:
            procrange["start"] = args.start
        if args.end != None:
            procrange["end"] = args.end
            if (args.start != None) and (args.start > args.end):
                logger.logError("Start {} is after End {}".format(args.start,args.end))
                exit()
            
        if args.wrapper != None and ("(" in args.wrapper):
            try:
                wrapper = re.compile(args.wrapper)
            except Exception as err:
                print('Error parsing Wrapper "{}": {}'.format(args.wrapper,err))
                        
        logger.logInfo("Got "+paths[0])

        myClient = pymongo.MongoClient(args.URI) 
        mydb = myClient.get_default_database()

        mycol = mydb[args.coll]
        
        textentry = re.compile("^[0-9]{4,4}-[0-9]{2,2}-[0-9]{2,2}T")
        jsonentry = re.compile("^{\"t\":{\"\$date\"\:")
        
        namePat = None 
        if (args.namePat != None) and ("(" in args.namePat):
            try:
                namePat = re.compile(args.namePat)
            except Exception as err:
                print('Error parsing Name Pattern "{}": {}'.format(args.namePat,err))
        
                                       
            


        
        #f = open("/Users/peterwilliamson/testapp.log", "r")
        #f = open("/Users/peterwilliamson/projects/snapfish/mongodb.log.2", "r")
        for inpath in paths:
            fileEncoding = "ISO-8859-1" 
            if args.utfEncoding:
                fileEncoding= "utf-8"
            f = open(inpath, "r",encoding = fileEncoding)
            nodeType = None
            if namePat == None:
                shortName = os.path.basename(inpath)
            else:
                nameMatch = namePat.search(inpath)
                if (nameMatch == None) or (nameMatch.group(1) == None): 
                    print("No short name match using {} for {}".format(args.namePat,inpath))
                    shortName = os.path.basename(inpath)
                else:
                    shortName = nameMatch.group(1)
            #        if len(nameMatch.group()) > 1:
            #            nodeType = nameMatch.group(2)
            print("Processing {} imported as {}".format(inpath,shortName))
            docBuf = []
            headers = {"isNew": False}
            
            #Readahead  the first line
            lineBuffer = f.readline().rstrip()  
            linecount = 0
            lastentry = None
            reportwrapfail = True
            ret = None
            #loop reading next line, current is in x
            for newline in f:
              linecount += 1
              if (linecount % 10000) == 0:
                  if lastentry != None:
                      print("Line: {} at {}".format(linecount,lastentry))
                  else:
                      print("{} lines skipped".format(linecount))
              # next line is a new entry so process the buffer in x
              if wrapper != None:
                  nameMatch = wrapper.search(newline)
                  if (nameMatch == None) or (nameMatch.group(1) == None): 
                      if reportwrapfail:
                          logger.logWarning("No wrapper match using {} for {}".format(args.wrapper,newline))
                          reportwrapfail = False
                      else:
                          logger.logInfo("No wrapper match using {} for {}".format(args.wrapper,newline))
                  else:
                      newline = nameMatch.group(1)
              if textentry.match(newline) or jsonentry.match(newline):
                  if textentry.match(lineBuffer):
                      try:
                          importText.logger = logger
                          ret = importText.process(lineBuffer,inpath,linecount,shortName,procrange,nodeType)
                      except Exception as progEx:
                        _, exceptionObject, tb  = sys.exc_info()
                        stackSummary = traceback.extract_tb(tb,6)
                        frameSummary = stackSummary[0]
                        for frame in stackSummary:
                            print(frame)
                        logline = '{}:{} in inputfile {} line {}.'\
                           .format(progEx.__class__.__name__,frameSummary.lineno,inpath,linecount)
                        logger.logInfo(stackSummary)
                        logger.logError(logline)
                        logger.logLine(lineBuffer)
                      else:
                          if ret != None:
                              if ret == True:
                                  break
                              lastentry = ret["ts"]
                              docBuf.append(fixDollar(ret))
                      if len(docBuf) >= 1000:
                          try:
                              mycol.insert_many(docBuf,ordered=False)
                          except Exception as err:
                              print(err)
    
                              print(docBuf)
                          docBuf[:] = []
                          
                      #done with x move the readahead into x 
                  elif jsonentry.match(lineBuffer):
                      importJson.logger = logger
                      ret = importJson.process(lineBuffer,inpath,linecount,shortName,procrange,headers,nodeType)
                      if ret != None:
                          if ret == True:
                              break
                          lastentry = ret["ts"]
                          if headers["isNew"]:
                              docBuf.append(fixDollar(headers))
                              headers["isNew"] = False
                          docBuf.append(fixDollar(ret))
                      if len(docBuf) >= 1000:
                          try:
                              mycol.insert_many(docBuf,ordered=False)
                          except Exception as err:
                              print(err)
    
                              print(docBuf)
                          docBuf[:] = []
                  lineBuffer = newline
              else:
                  # multi line log entry append 
                  lineBuffer += newline
            #Process the last line
            try:
                if ret != True:
                    if textentry.match(lineBuffer):
                        ret = importText.process(lineBuffer,inpath,linecount,shortName,procrange,nodeType)
                    elif jsonentry.match(lineBuffer):
                        ret = importJson.process(lineBuffer,inpath,linecount,shortName,procrange,headers,nodeType)
                    else:
                        ret = None
            except Exception as progEx:
                _, exceptionObject, tb  = sys.exc_info()
                stackSummary = traceback.extract_tb(tb,1)
                frameSummary = stackSummary[0]
                logline = '{}:{} in inputfile {} line {}.'\
                   .format(progEx.__class__.__name__,frameSummary.lineno,inpath,linecount)
                logger.logInfo(stackSummary)
                logger.logError(logline)
                logger.logLine(lineBuffer)
            else:
                if (ret != None) and (ret != True):
                    docBuf.append(ret) 
            if len(docBuf) > 0:
                try:
                    mycol.insert_many(docBuf,ordered=False)
                except Exception as err:
                    print(err)
                    print(docBuf)
                    
                docBuf[:] = []
            f.close()
        try:
            x = mydb["internal"].insert_one({"cmd": cmdTypes, "strings": strTypes, "keywords": keyTypes})
        except Exception as err:
            print(err)
        if len(jsonTypes) > 0:
            print(jsonTypes)
        return 0
    except KeyboardInterrupt:
        print('key')
        ### handle keyboard interrupt ###
        return 0
    except Exception as e:
     
        print('============')
        print(str(linecount)+": "+lineBuffer)
        raise(e)
        indent = len(program_name) * " "
        sys.stderr.write(program_name + ": " + repr(e) + "\n")
        sys.stderr.write(indent + "  for help use --help")
        return 2
    if len(jsonTypes) > 0:
        print(jsonTypes)
if __name__ == "__main__":
    sys.exit(main())    
          
          



