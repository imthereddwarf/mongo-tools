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
import bson.int64
import bson.regex
import bson.binary
import bson.objectid
import bson.dbref
import bson.code
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

class compDoc:
    mongoTypes = {type(None): "null", type(True): "boolean", str(int): "Integer32", str(bson.int64.Int64): "Integer64", 
                  str(float): "number", str(str): "string", str(list): "array", str(dict): "object",
                  str(datetime.datetime): "date", str(bson.regex.Regex): "regex", str(bson.binary.Binary): "binary",
                  str(bson.objectid.ObjectId): "objectid", str(bson.dbref.DBRef): "dbref", str(bson.code.Code): "Code",
                  str(bytes): "binary"}
    def __init__(self,doc,level=0):
        self.pos = 0
        self.sorted = sorted(doc.keys())
        self.data = doc
        self.current = None
        self.nesting = level+1
        return
    
    def nextKey(self):
        try:
            self.current = self.sorted[self.pos]
            self.pos += 1
            return self.current
        except:
            return None
        
    def getVal(self):
        return self.data[self.current]
    
    def getType(self):
        return type(self.data[self.current])
    
    def getArrayVal(self):
        return self.data[self.current][0]
    
    def getArrayType(self):
        return type(self.data[self.current][0])
    
    def getTypeName(self):
        pyType = str(type(self.data[self.current]))
        if pyType in self.mongoTypes:
            return self.mongoTypes[pyType]
        return pyType
    
    def getArrayTypeName(self):
        arr = self.data[self.current]
        if len(arr) > 0:
            pyType = str(type(arr[0]))
            if pyType in self.mongoTypes:
                return self.mongoTypes[pyType]
        else:
            return "Empty"
        return 
    
    def getSub(self):
        return compDoc(self.data[self.current],self.nesting)
    
    def getASub(self):
        return compDoc(self.data[self.current][0],self.nesting)
    
    def isSub(self):
        return (isinstance(self.data[self.current], dict))

    def isASub(self):
        return (isinstance(self.data[self.current][0], dict))
    
    def isArray(self):
        return (isinstance(self.data[self.current], list))
                   
    
def doCompare(obj1,obj2):
    isEqual = True
    gotTrailing = False
    changes = ""
    key1 = obj1.nextKey()
    key2 = obj2.nextKey()
    nesting = " "*3*obj1.nesting
    while (key1 != None) and (key2 != None):
        if key1 == key2:
            if obj1.getType() == obj2.getType():
                if isinstance(obj1.getArrayVal(),dict):
                    same, subChanges = doCompare(obj1.getSub(),obj2.getSub())
                    if not same:
                        if gotTrailing:
                            changes+= "\r\n"
                            gotTrailing = False;
                        changes += " {}{}: {{\r\n{}{}}}\r\n".format(nesting,key1,subChanges,nesting)
                        isEqual = False
                elif obj1.isArray():
                    same, subChanges = doCompareArr(obj1,obj2)
                    if not same:
                        if gotTrailing:
                            changes+= "\r\n"
                            gotTrailing = False;
                        changes += " {}{}: [\r\n{}{}]\r\n".format(nesting,key1,subChanges,nesting)
                        isEqual = False
                elif obj1.getVal() != obj2.getVal():
                    if gotTrailing:
                        changes+= "\r\n"
                        gotTrailing = False;
                    changes+= "<{}{}:{}\r\n".format(nesting,key1,obj1.getVal())
                    changes+=">{}{}:{}\r\n\r\n".format(nesting,key2,obj2.getVal())
                    isEqual = False
                    gotTrailing = False
            else:
                changes+="<{}{}:{}\r\n".format(nesting,key1,obj1.getTypeName())
                changes+=">{}{}:{}\r\n\r\n".format(nesting,key2,obj2.getTypeName())
                isEqual = False
                gotTrailing = False
            key1 = obj1.nextKey()
            key2 = obj2.nextKey()
        elif key1 > key2:
            changes += ">{}{}:{}\r\n".format(nesting,key2,obj2.getVal())
            key2 = obj2.nextKey()
            gotTrailing = True
            isEqual = False
        else:
            changes += "<{}{}:{}\r\n".format(nesting,key1,obj1.getVal())
            key1 = obj1.nextKey()
            gotTrailing = True
            isEqual = False

    return isEqual,changes

def doCompareArr(obj1,obj2):
    isEqual = True
    gotTrailing = False
    changes = ""
    nesting = " "*3*obj1.nesting
    if len(obj1.current) == 0:
        if len(obj2.current) == 0:
            return isEqual, changes
        else:
            return False, "<{}()\r\n>{}Empty\r\n".format(nesting,obj2.getArrayTypeName(),nesting)
    elif len(obj2.current) == 0:
        return False, "<{}Empty\r\n{}>{}{}\r\n".format(nesting,nesting,obj1.getArrayTypeName())
    

    if obj1.getArrayType() == obj2.getArrayType():
        if obj1.isASub():
            same, subChanges = doCompare(obj1.getASub(),obj2.getASub())
            if not same:
                if gotTrailing:
                    changes+= "\r\n"
                    gotTrailing = False;
                changes += " {}[{{\r\n{}{}}}]\r\n".format(nesting,subChanges,nesting)
                isEqual = False
        elif obj1.getArrayVal() != obj2.getArrayVal():
            if gotTrailing:
                changes+= "\r\n"
                gotTrailing = False;
            changes+= "<{}{}\r\n".format(nesting,obj1.getVal())
            changes+=">{}{}\r\n\r\n".format(nesting,obj2.getVal())
            isEqual = False
            gotTrailing = False
    else:
        changes+="<{}{}\r\n".format(nesting,obj1.getTypeName())
        changes+=">{}{}\r\n\r\n".format(nesting,obj2.getTypeName())
        isEqual = False
        gotTrailing = False

    return isEqual,changes
        
   
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
        parser.add_argument("--doc1", dest="doc1", help="Filter for document 1")
        parser.add_argument("--doc2", dest="doc2", help="Filter for document 1")
        parser.add_argument('-V', '--version', action='version', version=program_version_message)
        parser.add_argument('--URI', dest="URI", metavar='uri', help="MongoDb database URI for first or both document")
        parser.add_argument('--URI2', dest="URI2", metavar='uri', help="MongoDb database URI for second document")
        parser.add_argument('-c', '--coll', dest="coll", metavar='coll', help="Collection to import into")
        parser.add_argument('-c2', '--coll2', dest="coll2", metavar='coll', help="Collection for doc2")
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
        
           

        myClient = pymongo.MongoClient(args.URI) 
        mydb = myClient.get_default_database()
        mycol = mydb[args.coll]
        
        if args.URI2 != None:
            myClient2 = pymongo.MongoClient(args.URI)
        else:
            myClient2 = myClient
        
        mydb2 = myClient2.get_default_database()
        
        if args.coll2 != None:
            mycol2 = mydb2[args.coll2]
        else:
            mycol2 = mycol
            
        flt1 = json_util.loads(args.doc1, )
        flt2 = json.loads(args.doc2, object_hook=json_util.object_hook)
        
        doc1 = mycol.find_one(flt1)
        doc2 = mycol2.find_one(flt2)
        
        # Overall Cost
        

        same, changes = doCompare(compDoc(doc1),compDoc(doc2))
        if same:
            return 0
        else:
            print(changes)
            return 1


            
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