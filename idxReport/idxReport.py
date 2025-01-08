'''
Created on Nov 9, 2021

@author: peter.williamson

@copyright:  2020 organization_name. All rights reserved.

@license:    license

@contact:    user_email
@deffield    updated: Updated
'''

import pymongo
from bson import json_util
import datetime
import sys, os
from builtins import input
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
import traceback
import csv
import json
from dns.rdataclass import NONE




__all__ = []
__version__ = 0.1
__date__ = '2021-08-10'
__updated__ = '2021-08-10'



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

       
    
def keyrepl(matchobj):
    if matchobj.group(1).contains('"'):
        if matchobj.group(1).contains("'"):
            return '"'+matchobj.group(1).replace('"',"'")+'"'
        else:
            return "'"+matchobj.group(1).replace('"',"'")+"'"
    else:
        return '"'+matchobj.group(1).replace('"',"'")+'"'

def addKey(keyName,outstr,outDoc):
    if outstr != "":
        if keyName in outDoc:
            try:
                outDoc[keyName]["_text"] = outstr[1:]
            except Exception:
                pass
        else:
            try:
                intval = int(outstr[1:])
                outDoc[keyName] = intval
            except ValueError:
                try:
                    floatVal = float(outstr[1:])
                    outDoc[keyName] = floatVal
                except ValueError:
                    outDoc[keyName] = outstr[1:]
                    
def doQuote(token,c,alwaysQuote=False):
        gotQuote = "'" in token;
        gotDouble = '"' in token;
        if gotDouble:
            outstring = '"' + token.replace('"',"'") + '"'+ c;
        elif gotQuote | alwaysQuote:
            outstring = '"' + token + '"'+ c;
        else:
            outstring = token + c;
        return(outstring)
    
def addFieldValue(fields,key,output,default=None):
    if key in fields:
        if isinstance(fields[key],list) and (len(fields[key]) == 1):
            output.append(fields[key][0])
        else: 
            output.append(fields[key])
    else:
        if default is None:
            output.append('')
        else:
            output.append(default)
    
def writeResults(reportwriter,title,mycol,pipeline,isMongos=False):
    
    logger.logInfo(pipeline)
    
    reportwriter.writerow([title])
    reportwriter.writerow(["="*len(title)])
    if isMongos:
        headers = ["Collection","Operation","Filter","Count","Cost","Avg Ratio","min Time","max Time","tot Read","tot Ret","min Shards","max shards","Plan(s)","Sample ID","Sample Filter"]
    else:
        headers = ["Collection","Operation","Filter","Count","Cost","Avg Ratio","min Time","max Time","tot Read","tot Ret","Plan(s)","Sample ID","Sample Filter"]
    reportwriter.writerow(headers)
    cursor = mycol.aggregate(pipeline,allowDiskUse=True)
    for shape in  cursor:
        costRow = []
        addFieldValue(shape["_id"],"col",costRow)
        addFieldValue(shape,"Op",costRow)
        addFieldValue(shape["_id"],"filt",costRow)
        addFieldValue(shape,"cnt",costRow,default=0)
        addFieldValue(shape,"cost",costRow,default=0)
        addFieldValue(shape,"ratio",costRow,default=1)
        addFieldValue(shape,"minTime",costRow,default=1)
        addFieldValue(shape,"maxTime",costRow,default=1)
        addFieldValue(shape,"totalRead",costRow,default=-1)
        addFieldValue(shape,"totalRet",costRow,default=-1)
        if isMongos:
            addFieldValue(shape,"minShards",costRow,default=1)
            addFieldValue(shape,"maxShards",costRow,default=1)
        addFieldValue(shape,"plan",costRow)
        if "samp" in shape:
            sample = mycol.find_one({"_id": shape["samp"]})
            addFieldValue(sample,"_id",costRow)
            if sample["Command"] == "getMore":
                if "originatingCommand" in sample:
                    if "filter" in sample["originatingCommand"]:
                        addFieldValue(sample["originatingCommand"],"filter",costRow)
                    elif "pipeline" in sample["originatingCommand"]:
                        addFieldValue(sample["originatingCommand"],"pipeline",costRow)
            elif "filter" in sample["attr"]:
                addFieldValue(sample["attr"],"filter",costRow)
            elif "q" in sample["attr"]:
                addFieldValue(sample["attr"],"q",costRow)
            elif "pipeline" in sample["attr"]:
                addFieldValue(sample["attr"]["pipeline"],"q",costRow)
        reportwriter.writerow(costRow)
        

        
   
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
    try:
        # Setup argument parser
        parser = ArgumentParser(description=program_license, formatter_class=RawDescriptionHelpFormatter)
        parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", help="verbose, include Informational messages")
        parser.add_argument("--debug", dest="debug", action="store_true", help="debug")
        parser.add_argument("--mongos", dest="mongos", action="store_true", help="Data from Mongos")
        parser.add_argument("--infile", dest="infile", metavar="filter", help="Initial Match")
        parser.add_argument("--indexuse", dest="index", action="store_true", help="Index Stats")
        parser.add_argument('--outfile', '-o', dest="outFile", metavar='outFile', help="Output file")
        parser.add_argument('-V', '--version', action='version', version=program_version_message)
        parser.add_argument('--URI', dest="URI", metavar='uri', help="MongoDb database URI")
        parser.add_argument('-c', '--coll', dest="coll", metavar='coll', help="Collection to import into")
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
        
        matcher = {}
        if args.infile != None:
            matcher["infile"] = args.infile
        if (args.start != None) and (args.end != None):
            matcher["ts"] = {"$gt": args.start, "$lt": args.end}
        elif args.start != None:
            matcher["ts"] = {"$gt": args.start}
        elif args.end != None:
            matcher["ts"] = {"$lt": args.end}
        
        if matcher != {}:
            mtchSlow["$match"].update(matcher)
        matchstage = {"$match": matcher}
        print(matcher)
        print (mtchSlow)
        logger = myLogger("logParser",severity=logLevel)

        csvfile = sys.stdout
        if args.index:
            if args.outFile != None:
                basepath = args.outFile.replace(".csv","")
                indexfile = open(basepath+".ind.csv", "w", newline='')
                colfile = open(basepath+".col.csv", "w", newline='')
                dbfile = open(basepath+".db.csv", "w", newline='')
            else:
                indexfile = colfile = dbfile = csvfile
            indexwriter = csv.writer(indexfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            colwriter = csv.writer(colfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            dbwriter = csv.writer(dbfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        else:
            if args.outFile != None:
                csvfile = open(args.outFile, "w", newline='')
            reportwriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            

        myClient = pymongo.MongoClient(args.URI) 
        mydb = myClient.get_default_database()

        mycol = mydb[args.coll]
        
        # Overall Cost
        


        if args.mongos:
            writeResults(reportwriter,"Overall Cost",mycol,[matchstage,grpBynShards,srtByCost,lmt20],isMongos=True)
            logger.logInfo("Done Overall")
            writeResults(reportwriter,"Long Running",mycol,[mtchSlow,grpBynShards,srtByTime,lmt20],isMongos=True)
            logger.logInfo("Done Long")
            writeResults(reportwriter,"Multi Shard",mycol,[matchstage,grpBynShards,srtBynShards,lmt20],isMongos=True)
            logger.logInfo("Done Multi Shard")
            writeResults(reportwriter,"Multi Shard > 1",mycol,[matchstage,grpBynShards,excludeCntOne,srtBynShards,lmt20],isMongos=True)
            logger.logInfo("Done Multi Shard > 1")
        elif args.index:
            writeIndexResults(indexwriter,colwriter,dbwriter,mycol)
        else:
            selector = grpByShape
            if args.multi:
                selector = grpByColShape
            writeResults(reportwriter,"Overall Cost",mycol,[matchstage,mtchRatio,selector,srtByCost,lmt20])
            logger.logInfo("Done Overall")
            writeResults(reportwriter,"Collection Scans",mycol,[matchstage,mtchColScan,selector,srtByCost,lmt20])
            logger.logInfo("Done COLLSCAN")
            writeResults(reportwriter,"Long Running",mycol,[mtchSlow,selector,srtByTime,lmt20])
            logger.logInfo("Done Long")
            writeResults(reportwriter,"Has Sort Stage",mycol,[matchstage,mtchSort,selector,srtByCost,lmt20])
            logger.logInfo("Done Sort")


            
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