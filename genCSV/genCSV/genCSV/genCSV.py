'''

@author:     Pete Williamson

@copyright:  2025 organization_name. All rights reserved.

@license:    license

@contact:    user_email
@deffield    updated: Updated
'''

import pymongo
from bson import json_util
from bson.codec_options import CodecOptions
from pytz import timezone
import datetime
import sys, os
from argparse import ArgumentParser
from argparse import ArgumentTypeError
from argparse import RawDescriptionHelpFormatter
import traceback
import csv
import json
import re






__all__ = []
__version__ = 0.1
__date__ = '2021-08-10'
__updated__ = '2021-08-10'


bymin = {
        '$addFields': {
            'period': {
                '$toLong': {
                    '$divide': [
                        {
                            '$toLong': '$ts'
                        }, 60000
                    ]
                }
            }
        }
    }

bysec = {
        '$addFields': {
            'period': {
                '$toLong': {
                    '$divide': [
                        {
                            '$toLong': '$ts'
                        }, 1000
                    ]
                }
            }
        }
    }

dogroup = {
        '$group': {
            '_id': '$period', 
            'Total': {
                '$sum': 1
            },
        } 
    }

dosort = {'$sort': {'_id': 1}}
        
doprojmin =  {
        '$project': {
            '_id': 0,
            'Time': {
                '$toDate': {
                    '$multiply': [
                        '$_id', 60000
                    ]
                }
            }, 
        }
    }

doprojsec =  {
        '$project': {
            '_id': 0,
            'Time': {
                '$toDate': {
                    '$multiply': [
                        '$_id', 1000
                    ]
                }
            }, 
        }
    }


xtGroup =  {
        '$group': {
            '_id': {'p': '$period', 'subset': 'tbd'}, 
            'Total': {
                '$sum': 1
            }, 
        }
    }
        
xtConv =  {
        '$addFields': {
            'ss': {
                '$arrayToObject': '$nums'
            }
        }
    }

xtprojmin =  {
        '$project': {
            '_id': 0,
            'Time': {
                '$toDate': {
                    '$multiply': [
                        '$_id.p', 60000
                    ]
                }
            }, 
            'Item': {"$ifNull": ["$_id.subset","Other"]},
            'Total': 1
        }
    }

xtprojsec =  {
        '$project': {
            '_id': 0,
            'Time': {
                '$toDate': {
                    '$multiply': [
                        '$_id.p', 1000
                    ]
                }
            }, 
            'Item': {"$ifNull": ["$_id.subset","Other"]},
            'Total': 1
        }
    }



logger = None



bigInt = 0x8FFFFFFF
smallInt = 0xF0000000

keyTypes = {}
cmdTypes = {}
strTypes = {}


myClient = None
mydb = None
mycol = None

   
class myLogger:
    
    DEBUG = 0
    INFO = 1
    MESSAGE = 2
    WARNING = 3
    ERROR = 4
    FATAL = 5
    
    
    def __init__(self,host,datafile=None,syslogd=None,file=None,severity=None):
        self.errorText = ["Debug", "Info", "Message", "Warning", "Error", "Fatal"]
        self.myName = host
        self.failedLines = None
        self.syslog = syslogd
        self.maintWindowId = None
        if file is None:
            self.file = sys.stderr
        elif file == "-":
            self.file = sys.stdout
        elif isinstance(file,str):
            try:
                f = open(file,"w")
            except Exception as e:
                print("Error opening {}, {}. Using stderr for output".format(file,e))
                self.file = sys.stderr
            else:
                self.file = f
        else:
            print("Logger: file parameter must be a string or None, using stderr.",sys.stderr)
            self.file = file

        if isinstance(datafile,str):
            try:
                f = open(datafile,"w")
            except Exception as e:
                print("Error opening {}, {}. Using stderr for output".format(datafile,e))
                self.file = sys.stderr
            else:
                self.failedLines = f
        elif datafile != None:
            print("Logger: datafile parameter must be a string or None, using None",sys.stderr)

        if (severity is None) or (severity < 0) or (severity > 5):
            self.sevLevel = self.MESSAGE
        else:
            self.sevLevel = severity
        return
    
    def logDebug(self,message):
        if self.DEBUG >= self.sevLevel:
            print("DEBUG: {}".format(message),file=self.file)
        return
    
    def logInfo(self,message):
        if self.INFO >= self.sevLevel:
            print("INFO: {}".format(message),file=self.file)
        return    
    
    def logMessage(self,message,logDB=False):
        if self.MESSAGE >= self.sevLevel:
            print(message,file=self.file)
        if logDB:
            sevString = self.errorText[self.MESSAGE]
            self.db.logEvent(self.myName,sevString,message,self.maintWindowId)
        return
    
    def logWarning(self,message,logDB=False):
        if self.WARNING >= self.sevLevel:
            print("WARNING: {}".format(message),file=self.file)
        if logDB:
            sevString = self.errorText[self.WARNING]
            self.db.logEvent(self.myName,sevString,message,self.maintWindowId)
        return

    def logError(self,message,logDB=False):
        if self.ERROR >= self.sevLevel:
            print("ERROR: {}".format(message),file=self.file)
        if logDB:
            sevString = self.errorText[self.ERROR]
            self.db.logEvent(self.myName,sevString,message,self.maintWindowId)
        return
    
    def logFatal(self,message,logDB=False):
        if self.MESSAGE >= self.sevLevel:
            print("FATAL: {}".format(message),file=self.file)
        if logDB:
            sevString = self.errorText[self.FATAL]
            self.db.logEvent(self.myName,sevString,message,self.maintWindowId)
        return
    
    def logLine(self,failedLine):
        if self.failedLines != None:
            print(failedLine,file=self.failedLines)
    
    def logProgress(self):
        self.file.write(".")
        self.file.flush()
    
    def logComplete(self):
        print(" ",file=self.file)


    
def writeResults(reportwriter,titles,cols,mycol,pipeline,granularity):
    
    logger.logInfo(pipeline)
    emptyrow = []
    reportwriter.writerow(titles)
    cursor = mycol.aggregate(pipeline,allowDiskUse=True)
    nextRow = None
    for stat in  cursor:
        dataRow = []
        if nextRow != None and nextRow < stat[cols[0]]:
            while nextRow < stat[cols[0]]:
                dataRow = [int(nextRow.timestamp())]
                for n in range(len(cols)-1):
                    dataRow.append(0)
                reportwriter.writerow(dataRow)
                nextRow = nextRow + datetime.timedelta(seconds = granularity)
        nextRow = stat[cols[0]] + datetime.timedelta(seconds = granularity)
        dataRow = []
        for col in cols:
            if col in stat:
                if isinstance(stat[col], datetime.datetime):
                    dataRow.append(int(stat[col].timestamp()))
                else:
                    dataRow.append(stat[col])
            else:
                dataRow.append("")
        reportwriter.writerow(dataRow)
        
def getResults(reportwriter,titles,cols,mycol,pipeline,granularity):

    logger.logInfo(pipeline)
    emptyrow = []

    cursor = mycol.aggregate(pipeline,allowDiskUse=True)
    cols = []
    myTitles = [titles[0]]
    titleNames = []
    values = []
    nextRow = None
    for res in cursor:
        if nextRow == None:
            nextRow = {"Time": res["Time"]}
        elif res["Time"] != nextRow["Time"]:
            values.append(nextRow)
            nextRow = {"Time": res["Time"]}
        if not res["Item"] in cols:
            cols.append(res["Item"]) 
            coldesc = "{} {}#units={};section={}".format(res["Item"],titles[1],titles[2],titles[3])
            myTitles.append(coldesc)  
            titleNames.append(res["Item"])
        nextRow[res["Item"]] = res["Total"]
    if len(nextRow) > 1:
        values.append(nextRow)
    reportwriter.writerow(myTitles)
    nextRow = None
    haveData= True
    nextRow = None
    for stat in values:  
        dataRow = []
        if nextRow != None and nextRow < stat["Time"]:
            while nextRow < stat["Time"]:
                dataRow = [int(nextRow.timestamp())]
                for n in range(len(cols)):
                    dataRow.append(0)
                reportwriter.writerow(dataRow)
                nextRow = nextRow + datetime.timedelta(seconds = granularity)
        nextRow = stat["Time"] + datetime.timedelta(seconds = granularity)
        dataRow = [int(stat["Time"].timestamp())]
        for col in cols:
            if col in stat:
                if isinstance(stat[col], datetime.datetime):
                    dataRow.append(int(stat[col].timestamp()))
                else:
                    dataRow.append(stat[col])
            else:
                dataRow.append("")
        reportwriter.writerow(dataRow)

        
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
    for parm in argv:
       print(parm)
       
    try:
        # Setup argument parser
        parser = ArgumentParser(description=program_license, formatter_class=RawDescriptionHelpFormatter)
        parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", help="verbose, include Informational messages")
        parser.add_argument("-m", "--minutes", dest="min", action="store_true", help="Minute Resolution")
        parser.add_argument("-a", "--all", dest="doall", action="store_true", help="Include all queries")
        parser.add_argument("--debug", dest="debug", action="store_true", help="debug")
        parser.add_argument("--filter", dest="filter", metavar="filter", help="Initial Match", nargs=1)
        parser.add_argument("--section", dest="section", metavar="filter", help="Initial Match", nargs=1)
        parser.add_argument("--metrics", dest="metrics", metavar="filter", help="Name:unit:var:op:field", nargs="+")
        parser.add_argument("--startdate", dest="start", metavar="Start From", help="The Start Date - format YYYY-MM-DD", type=valid_date)
        parser.add_argument("--enddate", dest="end", metavar="Start From", help="The Start Date - format YYYY-MM-DD", type=valid_date)
        parser.add_argument('--outfile', '-o', dest="outFile", metavar='outFile', help="Output file")
        parser.add_argument('--crosstab', '-cx', dest="xtab", help="Do Xtab", nargs=1)
        parser.add_argument('-V', '--version', action='version', version=program_version_message)
        parser.add_argument('--URI', dest="URI", metavar='uri', help="MongoDb database URI")
        parser.add_argument('-c', '--coll', dest="coll", metavar='coll', help="Collection to export from")
        #parser.add_argument(dest="paths", help="paths to folder(s) with source file(s) [default: %(default)s]", metavar="path", nargs='+')

        # Process arguments
        args = parser.parse_args()
        #print(args.URI)
        #paths = args.paths

        if args.debug:
            logLevel = myLogger.DEBUG
        elif args.verbose:
            logLevel = myLogger.INFO
        else:
            logLevel = myLogger.MESSAGE
        
        logger = myLogger("logParser",severity=logLevel)
        
        matcher = {}
        if args.filter != None:
            try:
                matcher = json.loads(args.filter[0], object_hook=json_util.object_hook)
            except Exception as err:
                logger.logError("===========\n{}\n{}\n===========".format(args.filter[0],err))     
                return 1   
        
        if (args.start != None) and (args.end != None):
            matcher["ts"] = {"$gt": args.start, "$lt": args.end}
        elif args.start != None:
            matcher["ts"] = {"$gt": args.start}
        elif args.end != None:
            matcher["ts"] = {"$lt": args.end}
        
        matchstage = {"$match": matcher}
        
        myClient = pymongo.MongoClient(args.URI) 
        mydb = myClient.get_default_database().with_options(codec_options=CodecOptions(tz_aware=True,tzinfo=timezone("UTC")))

        mycol = mydb[args.coll]
        

        cols = ["Time"]
        Titles = ["Time"]
        if args.xtab != None:
            parts = args.metrics[0].split(":")
            Titles = ["Time",parts[0],parts[1],args.section[0]]
        else:
            for metric in args.metrics:
                parts = metric.split("|")
                logger.logInfo("Metric: {}".format(metric))
                coldesc = "{}#units={};section={}".format(parts[0],parts[1],args.section[0])
                Titles.append(coldesc)
                cols.append(parts[2])
                if len(parts) > 3:
                    aggfield = parts[4]
                    print(aggfield)
                    if aggfield.startswith("{"):
                        try:
                            aggfield = json.loads(parts[4], object_hook=json_util.object_hook)
                        except Exception as err:
                            logger.logError("===========\n{}\n{}\n===========".format(parts[4],err))     
                            return 1   
                    dogroup['$group'][parts[2]] = {parts[3]: aggfield}
                    if args.min:
                        doprojmin["$project"][parts[2]] = 1
                    else:
                        doprojsec["$project"][parts[2]] = 1
                    


        
        logger.logInfo("Selection Filter: {}".format(matcher))
           

        csvfile = sys.stdout


        if args.outFile != None:
            csvfile = open(args.outFile, "w", newline='')
        reportwriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            

        logger.logInfo("URI: {}, Collection {}.".format(args.URI,args.coll))
        

        
        if not args.xtab:
            if args.min:
                doprojmin["$project"]["Total"] = 1
                writeResults(reportwriter,Titles,cols,mycol,[matchstage,bymin,dogroup,dosort,doprojmin],60)
            else:
                doprojsec["$project"]["Total"] = 1
                writeResults(reportwriter,Titles,cols,mycol,[matchstage,bysec,dogroup,dosort,doprojsec],1)
        else:
            print(xtGroup["$group"]["_id"])
            xtGroup["$group"]["_id"]["subset"] =  args.xtab[0]
            print(type(xtGroup["$group"]["_id"]))
            if args.min:
                restab = getResults(reportwriter,Titles,cols,mycol,[matchstage,bysec,xtGroup,dosort,xtprojmin],60)
            else:
                restab = getResults(reportwriter,Titles,cols,mycol,[matchstage,bysec,xtGroup,dosort,xtprojsec],1)
        logger.logInfo("Done Overall")



            
        return 0
    except KeyboardInterrupt:
        print('key')
        ### handle keyboard interrupt ###
        return 0
    except Exception as e:
     
        print('============')
        raise(e)
        indent = len(program_name) * " "
        sys.stderr.write(program_name + ": " + repr(e) + "\n")
        sys.stderr.write(indent + "  for help use --help")
        return 2

if __name__ == "__main__":
    sys.exit(main())    
          
          

