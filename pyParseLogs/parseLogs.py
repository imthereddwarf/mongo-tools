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
import json, re
import simplejson
import datetime
import sys, os
from builtins import input
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from argparse import ArgumentTypeError
import traceback




__all__ = []
__version__ = 0.1
__date__ = '2020-12-22'
__updated__ = '2020-12-22'

formats = ["WRITE-remove-command-planSummary-keysExamined-docsExamined-ndeleted-keysDeleted-numYields-locks",
"WRITE-remove-command-planSummary-keysExamined-docsExamined-ndeleted-numYields-locks",
"CONTROL-Sessions-interval",
"COMMAND-command-command-originatingCommand-planSummary-cursorid-keysExamined-docsExamined-numYields-nreturned-reslen-locks-protocol",
"WRITE-update-command-planSummary-keysExamined-docsExamined-nMatched-nModified-keysInserted-keysDeleted-numYields-locks",
"COMMAND-command-command-numYields-reslen-locks-protocol",
"COMMAND-command-command-planSummary-keysExamined-docsExamined-numYields-reslen-locks-protocol",
"COMMAND-command-command-planSummary-keysExamined-docsExamined-hasSortStage-cursorExhausted-numYields-nreturned-reslen-locks-protocol",
"WRITE-update-command-command-q-user_id-u-$set-ts_u-$inc-ncount-$push-notes-nadminaccounttype-nadminid-nid-ntext",
"WRITE-update-command-planSummary-keysExamined-docsExamined-nMatched-nModified-upsert-keysInserted-numYields-locks",
"WRITE-update-command-planSummary-keysExamined-docsExamined-nMatched-nModified-numYields-locks",
"WRITE-update-command-planSummary-keysExamined-docsExamined-nMatched-nModified-keysInserted-numYields-locks",
"NETWORK-received",
"WRITE-update-command-planSummary-keysExamined-docsExamined-nMatched-nModified-keysInserted-keysDeleted-writeConflicts-numYields-locks",
"WRITE-update-command-command-q-user_id-u-$set-ts_u-$inc-ncount-$push-notes-nadminaccounttype-other_user_ids-nadminid-nid-ntext-Exp-file-https",
"COMMAND-command-command-planSummary-keysExamined-docsExamined-cursorExhausted-numYields-nreturned-reslen-locks-protocol",
"WRITE-update-command-planSummary-keysExamined-docsExamined-nMatched-nModified-keysInserted-writeConflicts-numYields-locks",
"COMMAND-command-command-planSummary-keysExamined-docsExamined-nMatched-nModified-keysInserted-keysDeleted-numYields-reslen-locks-protocol",
"COMMAND-command-command-ninserted-keysInserted-numYields-reslen-locks-protocol",
"COMMAND-serverStatus-slow]"]


dotKey = re.compile("([^.:{}, ]*\\.[^: },]*):");
#jsonKey = re.compile("{\\s*([\"']?)(([^\"']|\\\\[\"'])*)\1\s*:");
jks = "([{,]\\s*)([^'\"\\s][^:]*)(\\s*:)"
#print(jks)
jsonKey = re.compile("([{,]\\s*)([^'\"\\s][^:]*)(\\s*:)")
jsonValue = re.compile(":\\s*([^ {},\"'][^},]*|\"([^\"]|\\\")*\"|'([^']|\\')*')");
imbedquote = re.compile(": *(\"[^\"]*\"[^\"]*\")([,}])");
smallNumber = re.compile(": *([+-]?[0-9]+e-?[0-9]+.[0-9]+)");
bindata = re.compile("BinData\\([0-9], ([0-9A-F.]*)\\)");
connection = re.compile("\[conn([0-9]*)\]");
ipport = re.compile("([0-9]*\.[0-9]*\.[0-9]*\.[0-9]*):([0-9]*)")
logKey = re.compile("^[a-zA-Z]*:$")
embedcolon = re.compile("^:({[ }]|[0-9]* )")
embedcolonhex = re.compile("^:({|[0-9A-Fa-f]*) ")
floating = re.compile("^[+-]?[0-9.]+[eE][+-]?[0-9.]+")
digits = re.compile("([0-9]+)")
exTime = re.compile("([0-9]+)ms")
exRange = re.compile("([0-9]+)-([0-9]*)")
notEscapedDouble  = re.compile("[^\\\\]\"")
logger = None
jsonEscapes = ["b","f","n","r","t",'"',"\\"]

logKeywords = {"command:": "String", "numYields:": 9, "reslen:": 9, "nShards:": 9, "ninserted:": 9, "nreturned:": 9,
"cursorExhausted:": 9, "nMatched:": 9, "nModified:": 9, "cursorid:": 9, "originatingCommand:": "String",
"locks:": "JSON", "storage:": "JSON", "planSummary:": "String", "keysExamined:": 9,
"docsExamined:": 9, "flowControl:": "JSON", "writeConflicts:": 9, "keysInserted:": 9,
"keysDeleted:": 9, "appName:": "String", "hasSortStage:": 9, "prepareReadConflicts:": 9, "fromMultiPlanner:": 9,
"ndeleted:": 9, "planCacheKey:": 9, "queryHash:": 9}

cmdMessages = {"successfully": {"cmdType": "setParam", "newValue": 5, "oldValue": 7, "paramName": 3},
               "serverStatus": {"cmdType": "serverStatus", "Value": 3, "Data": 4},
               "mongos": {"cmdType": "mongos", "Value": "1-"}}

filterLocations = {"update": "updates.q", "count": "query", "find": "filter", "aggregate": "pipeline", "delete": "deletes.q", 
                    "findAndModify": "query", "command": "q"}

updateLocations = {"command": "u", "update": "updates.u", "findAnModify": "update"}

sortLocations = {"command": "u", "update": "updates.u", "findAndModify": "sort", "find": "sort"}
               
uKeys = ["multi:","projec","shardV"]

bigInt = 0x8FFFFFFF
smallInt = 0xF0000000

keyTypes = {}
cmdTypes = {}
strTypes = {}
isMQL = {"\"filter\":": 1, "\"q\":":1, "\"u\":":1, "\"pipeline\":": 1, "\"sort\":": 1, "\"query\":": 1, "\"planSummary\":": 1, "\"appliedOp\":": 1,
         "\"originatingCommand\":\"pipeline\"": 1,"\"originatingCommand\":\"filter\"": 1}
goodCMD = ["insert", "update", "getMore","find","aggregate","commitTransaction","findAndModify"]

myClient = None
mydb = None
mycol = None

class jsonVals:
    def __init__(self):
        self.jsonVal = []
        self.jsonData = []
        
    def add(self,jsontxt):
        self.jsonVal.append(jsontxt)
        pos = len(self.jsonVal)-1
        newstr = "JSON" + str(pos)
        try:
            data = json.loads(jsontxt, object_hook=json_util.object_hook)
            self.jsonData.append(data)
        except Exception as err:
            #print(type(err))
            #(cleaned,lastpos) = cleanJSON(snip,0)
            try:
                data = simplejson.loads(jsontxt)
                self.jsonData.append(data)
            except Exception as err:
                logger.logInfo("===========\n{}\n{}\n===========".format(jsontxt,err))
                self.jsonData.append({})
        return newstr
    
    def getJsonVal(self,param,keyName=""):
        pos = -1
        if isinstance(param,int):
            pos = param
        elif isinstance(param,str):
            jvmatch = digits.search(param)
            if jvmatch != None:
                try:
                    pos = int(jvmatch.group(0))
                except:
                    pos = -1
        if (pos < 0) or (pos >= len(self.jsonVal)) or (pos >= len(self.jsonData)):
            print("Invalid parameter {} ".format(param))
            return None
        if "\""+keyName+"\":" in isMQL:
            return " " + doQuote(self.jsonVal[pos],"",True)
        return self.jsonData[pos]
   
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
                f = open(datafile,"w",newline='')
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
    
def cleanJSON(input,startpos,myPath=""):
    #print(input[startpos:])
    if (input[startpos:startpos+1] == "{"):
        inkey = True;
        inarray = False;
        quoted = ""
        outstring = input[startpos:startpos+1]
        startpos += 1
    elif (input[startpos:startpos+1] == "["):
        inkey = False;
        inarray = True;
        quoted = "[]"
        outstring = input[startpos:startpos+1]
        startpos += 1
    else:
        inkey = False;
        inarray = False;
        outstring = ""
        quoted = ""
    isescape = False;
    stringValue = False
    instring = '\0';
    token = "";
    saveAsString = 0
    endpos = len(input);
    
    i = startpos-1
    while (i < endpos-1):
        i += 1
        c = input[i:i+1]
        # end of sub object
        if  (c == ',') & (token == ""):
            outstring += c;
            if not inarray:
                inkey = True
            continue;
        # Escape remember and con
        if c == '\\':
            isescape = True;
            continue;
        # Escape plus start of string, remove the escape for now
        if (c == instring) & isescape:
            isescape = False;
            token += c;
            continue;
        elif isescape:  # Not escaping a quote
            if c in jsonEscapes:
                token += "\\"+c
            else:
                token += c
            isescape = False
        # We're inside a string
        if instring != '\0':
            if token[-10:] == "appMessage":
                print(token[-10:])
            if saveAsString != 0:  # were saving JSON as a String
                if (c == instring):
                    #print(token)
                    token += "\\"+ c   # escape the quotes
                    print(token[-4:])
                    if token[-4:] == ": \\"+c:
                        stringValue = True 
                    else:
                        stringValue = False
                elif (c == "{") | (c == "["): # nested array or sub doc
                    #print(input[i:i+5])
                    if saveAsString == -1:    #This is the first open
                        saveAsString = 1
                        stringValue = False  #Not in an embeded string
                        token = '"'
                    else:
                        saveAsString += 1
                    token += c
                elif ((c == "}") | (c == "]")) and (not stringValue):
                    if saveAsString > 1:    # end of nested object
                        saveAsString -= 1 
                        token += c
                        if input[i+3:i+9] in uKeys:
                            logger.logDebug("mismatched quote before {}, line: {} ".format(input[i+3:i+9],input[:23]))
                            saveAsString = 0
                            token += '"'
                            print(token)
                            instring = '\0'
                    elif saveAsString == 1:                    # end of JSON
                        saveAsString = 0
                        token += c + '"'
                        instring = '\0'
                    else:
                        token += c
                elif (c < ' ') | (c > '~') : # unprintable
                    print(hex(ord(c)))
                    print("{0:#0{1}x}".format(ord(c),6))
                    token += "{0:#0{1}x}".format(ord(c),6).replace("0x","\\u")
                else:
                    token += c
                    
            elif c == instring:
                instring = '\0';  # string closed
                token += '"';
            elif (c < ' ') | (c > '~') : # unprintable
                print(hex(ord(c)))
                token += "{0:#0{1}x}".format(ord(c),6).replace("0x","\\u")
            else:
                if isescape:
                    token += "\\"
                    isescape = False
                token += c
            continue
        elif ((c == '"') | (c ==    "'")):
            instring = c;
            token += '"';
            continue
        #array delimiter 
        elif (c == '[') :
            (arrString, nextpos) = cleanJSON(input, i,myPath+"[]")
            outstring += arrString
            i = nextpos
            inkey = False;
            continue;
        
        # coalesce whitespace outside strings
        if (c == ' ') | (c == '\t'):
            iswhitespace = True; 
            continue;      
        
        if c == '{':
            (objectString, nextpos) = cleanJSON(input, i,myPath+quoted)
            outstring += objectString
            i = nextpos
            inkey = False
            continue
        elif ((c == '}') | (c == ',') | (c == ']')) & (not inkey):
            # quoted string 
            if ((token[:1] == '"') | (token[:1] == '\'')) & (token[:1] == token[-1:]): 
                if ((token[:1] == '"') and notEscapedDouble.match(token[1:-1])) or \
                    ((token[:1] == "'") and notEscapedSingle.match(token[1:-1])):
                    outstring += token[:1] + token[1:-1].replace(token[:1],"\\"+token[:1]) + token[-1] + c;
                else:
                    outstring += token + c;
                token = "";
            # string starts with quote but is not closed
            elif ((token[:1] == '"') | (token[:1] == '\'')) :
                print(token)
                quotechar = token[:1];
                if quotechar == '"':
                    notquote = '\'';
                else:
                    notquote = '"';
                if quotechar in token[1:]:
                    outstring += quotechar + token[1:].replace(quotechar,notquote) + quotechar+c;
                else:
                    outstirng += token + quotechar+c;
            elif (token.startswith("/") & token.endswith("/")):  # regex
                outstring += doQuote(token,c,True)
            # Not a string
            else:   
                if token.startswith("new"):
                    outstring += '"' + token + '"'+c;
                elif token.startswith("MinKey") | token.startswith("MaxKey"):
                    outstring += '"' + token + '"'+c;
                elif token.startswith("BinData("):
                    if not token.endswith(")"):
                        token += c
                        continue
                    else:
                        outstring += doQuote(token,c,True)
                elif token.startswith("Timestamp("):
                    if not token.endswith(")"):
                        token += c
                        continue
                    else:
                        outstring += doQuote(token,c,True)
                elif floating.match(token):
                    outstring += doQuote(token,c,True)
                else:
                    outstring += doQuote(token,c)
            token = "";
            if ( c == ']'):
                return(outstring,i)
            elif ( c == '}'):
                return(outstring,i)
            else:
                if not inarray:
                    inkey = True
        # no key found treet as junk or empty
        elif ((c == '}') |  (c == ']')) & inkey:
            if token == "":
                outstring += c
            else:
                outstring += doQuote(token,c)
            return(outstring,i)
        # end of key    
        elif ((c == ":") & inkey & (instring == '\0')):
            #print(token)
            if ((token[:1] == '"') | (token[:1] == '\'')) & (token[:1] == token[-1:]): 
                if token[:1] in token[1:-2]:
                    quoted = token[:1] + token[1:-2].replace(token[:1],"\\"+token[:1])+":";
                else:
                    quoted = token+ ":";
                token = "";
            elif '"' not in token:  #No double quote use it
                quoted = '"' + token + '":';
            elif "'" not in token:  #no Single Quote use it
                quoted = "'" + token + "':";        
            else:   # Both single and double quotes, replace double with single and use double to quote
                quoted = '"' + token.replace('"',"'") + '":';
            if quoted[1:2] == "$":
                quoted = quoted[:1]+"_"+quoted[1:]
            inkey = False;
            if myPath+quoted in isMQL:   # save this JSON as a string
                saveAsString = -1  #Look for a starting {
                instring = '"'
                outstring += quoted 
            else:
                saveAsString = 0
                outstring += quoted
            token = "";
        else:
            #if (token == "") & (c == "$"):
            #    token = "_"
            token += c;
    return (outstring,i);


def parseLine(tok,j,jsonObj):
    outDoc = {}
    while j < len(tok):
        #print(tok[j])
        if tok[j].endswith(":") and (tok[j] in logKeywords):
            keyName = tok[j][0:-1]
            jj = j+1
            outstr = ""
            while not (tok[jj].endswith(":") & tok[jj].endswith(",")): # got a value
                if (jj == (len(tok)-1)) & (exTime.match(tok[jj]) != None):
                    addKey(keyName,outstr,outDoc)
                    etMatch = exTime.search(tok[jj])
                    outDoc["time"]=int(etMatch.group(1))
                    jj += 1
                    break
                elif not tok[jj].endswith(":"):
                    if tok[jj][0:4] == "JSON":
                        obj = jsonObj.getJsonVal(tok[jj],keyName)
                        if isinstance(obj,dict):
                            outDoc[keyName] = obj
                        elif isinstance(obj,str):
                            outstr += " " + obj
                        else:
                            outstr += " ???"
                    else:
                        outstr += " " + tok[jj]
                    jj += 1
                    if jj >= len(tok):
                        break
                else:
                    addKey(keyName,outstr,outDoc)
                    break
            tStr = tok[j].replace(".","_")
            if tStr in keyTypes:
                keyTypes[tStr] += 1
            else:
                keyTypes[tStr] = 1
            j=jj
        else:
            tStr = tok[j].replace(".","_")
            if tStr in strTypes:
                strTypes[tStr] += 1
            else:
                strTypes[tStr] = 1
            j=j+1
    return outDoc

def procLine(x,inpath,linecount):
    
    level = 0
    position = 0
    startPos = 0
    copied = 0
    newstr = ""
    jsonObj = jsonVals()
    i = -1
    endpos = len(x)
    while (i < endpos-1):
        i += 1
        c = x[i:i+1]
        if c == "{": 
            startPos = i
            newstr += x[copied:i]
            (jsontxt,newpos) = cleanJSON(x,i)
            i = newpos
            newstr += jsonObj.add(jsontxt)
            copied = i+1
        elif c == ":":
            if x[i+1:i+2] != " ":  #embeded colon
                #print(x[i:])
                gotmatch = embedcolon.match(x[i:])
                if gotmatch != None:
                    #print(gotmatch.group(1))
                    newstr += x[copied:i+1] + " "
                    copied = i+1
                else:
                    gotmatch = embedcolonhex.match(x[i:])
                    if gotmatch != None:
                        #print(gotmatch.group(1))
                        newstr += x[copied:i+1] + ' "' + gotmatch.group(1) + '"'
                        i += len(gotmatch.group(1))
                        copied = i+1
        elif c == '"':
            #print(x[i:])
            newstr += x[copied:i+1]
            i += 1
            while (x[i:i+1] != '"') & (i < endpos-1):
                if x[i:i+1] != " ":
                    newstr += x[i:i+1]
                i += 1
            newstr += '"'
            copied = i+1
                    
                   
        
                
    
            
    newstr += x[copied:]
    tok = newstr.split()
    
    
    if tok[2] == "NETWORK":
        outstr = "{ type: \"NETWORK\","
        if tok[4] == "connection":
            outstr += 'connection: "'+tok[8][1:]+'",'
            ipmatch = ipport.match(tok[7])
            outstr += 'ip_addr: "'+ipmatch.group(1)+'"}'
            #print(outstr)
        elif tok[4] == "end":
            connMatch = connection.match(tok[3])
            outstr += 'connection: "'+connMatch.group(1)+'",'
            ipmatch = ipport.match(tok[6])
            outstr += 'ip_addr: "'+ipmatch.group(1)+'"}'   
            #print(outstr)
        elif tok[4] == "received":
            outDoc = { "type": tok[2], "infile": inpath, "lineno": linecount }
            d = datetime.datetime.strptime(tok[0][0:23], "%Y-%m-%dT%H:%M:%S.%f")
            outDoc["ts"] = d
            connMatch = connection.match(tok[3])
            outDoc["connection"] = connMatch.group(1)
            ipmatch = ipport.match(tok[8])
            outDoc["ip_addr:"] = ipmatch.group(1) 
            outDoc["connection_info"] = jsonObj.getJsonVal(0)
            #return(outDoc)
        return None
    elif tok[2] == "CONTROL":
        outstr = ""
        #if (tok[4] == "db") & (tok[5] == "version"):
        #    outstr = "{ type: \"CONTROL\", version: \""+tok[6]+"\"}"
        return None
    elif tok[2] == "REPL":
        outDoc = { "type": tok[2], "infile": inpath, "lineno": linecount, "thread": tok[3] }
        d = datetime.datetime.strptime(tok[0][0:23], "%Y-%m-%dT%H:%M:%S.%f")
        outDoc["ts"] = d
        if (tok[4]+tok[5] == "appliedop:"):
            outDoc["appliedOP"] = tok[6]
            outDoc["Details"] = jsonObj.getJsonVal(0)
        else:
            j = 4
            outStr = ""
            while j < len(tok):
                try:
                    outStr += " " + tok[j]
                except Exception as err:
                    print(newstr)
                    print(j)
                    print("==")
                    print(outStr)
                    print(tok[j])
                j += 1
            outDoc["message"] = outStr[1:]
        return(outDoc)         
    elif (tok[2] == "COMMAND") | (tok[2] == "WRITE"):
        outDoc = { "type": tok[2], "infile": inpath, "lineno": linecount }
        #use ciso8601
        d = datetime.datetime.strptime(tok[0][0:28], "%Y-%m-%dT%H:%M:%S.%f%z")
        outDoc["ts"] = d
        connMatch = connection.match(tok[3])
        if connMatch != None:
            outDoc["connection"] =  connMatch.group(1)
        startpos = 4
        if (tok[startpos] in cmdMessages):  # This is a command warning message (eg serverStatus was very slow)
            procDoc = cmdMessages[tok[startpos]]
            for key in procDoc:
                if (type(procDoc[key]) == int):
                    parVal = tok[startpos+procDoc[key]]
                    if parVal.startswith("JSON"):
                        outDoc[key] = jsonObj.getJsonVal(parVal)
                    else:
                        outDoc[key] = parVal
                elif exRange.match(procDoc[key]):
                    matches = exRange.search(procDoc[key])
                    spos = startpos + int(matches.group(1))
                    epos = len(tok)
                    if matches.groups == 2:
                        if epos >  startpos + int(matches.group(2)):
                            epos = startpos + int(matches.group(2))                    
                else:
                    outDoc[key] = procDoc[key]
            return(outDoc)
        elif tok[startpos].startswith("JSON"):
            msgDoc = jsonObj.getJsonVal(tok[startpos])
            outDoc["Message"] = msgDoc
            return(outDoc)
        if len(tok) >= startpos+4:  # We have at least 4 tokens after the preamble
            if tok[startpos]+tok[startpos+1]+tok[startpos+2]+tok[startpos+3] == "warning:loglineattempted": # skip over the long line warning
                outDoc["longLine"] = tok[startpos+4]
                startpos += 14
        if startpos+1 < len(tok):
            outDoc["Object"] = tok[startpos+1]
        else:
            print(tok[0])
        if tok[startpos+2] == 'appName:':
            outDoc["application"] = tok[startpos+3]
            j = startpos + 4
        else:
            j = startpos+2
        
        if tok[2] == "COMMAND":
            if (j+1) < len(tok):
                tStr = tok[j+1].replace(".","_")
            else:
                tStr = "Missing"
        else:
            if j < len(tok):
                tStr = tok[j].replace(".","_")
            else:
                tStr = "Missing"
        if tStr in cmdTypes:
            cmdTypes[tStr] += 1
        else:
            cmdTypes[tStr] = 1

        if tok[j] == "command:":
            if tok[j+2].startswith("JSON"):
                cmdType = tok[j+1]
                cmdData = jsonObj.getJsonVal(tok[j+2])
            elif tok[j+1].startswith("JSON"):
                cmdType = tok[4]
                cmdData = jsonObj.getJsonVal(tok[j+1])
            else:
                print("Unexpected line format:")
                print(x)
                return None
            outDoc["Command"] = cmdType
            if isinstance(cmdData,dict):
                if cmdType in filterLocations:
                    fltTree = filterLocations[cmdType].split(".")
                    filter = cmdData
                    for key in fltTree:
                        parent = filter
                        if isinstance(parent,dict) and (key in parent):
                            filter = parent[key]
                        else:
                            filter = None
                            break
                        if isinstance(filter, list):
                            filter = filter[0]
                    if filter != None:
                        outDoc["filter_shape"],outDoc["filter_params"] = fmtQuery(filter)
                    #del parent[key]
                outDoc["attr"] = cmdData
            else:
                print("Error parsing command: in {} on line {}".format(inpath,linecount))
            j += 3

        lineData = parseLine(tok,j,jsonObj)
        if isinstance(lineData,dict):
            outDoc.update(lineData)
        if ("filter_shape" not in outDoc) and ("originatingCommand" in outDoc):
            if ("Command" in outDoc) and (outDoc["Command"] == "getMore") and ("filter" in outDoc["originatingCommand"]):
                outDoc["filter_shape"],outDoc["filter_params"] = fmtQuery(outDoc["originatingCommand"]["filter"])
            elif ("Command" in outDoc) and (outDoc["Command"] == "getMore") and ("pipeline" in outDoc["originatingCommand"]):
                outDoc["filter_shape"],outDoc["filter_params"] = fmtQuery(outDoc["originatingCommand"]["pipeline"])
        
        
            

#
# Now Calculate stuff
#
        if ("docsExamined" in outDoc) & ("nreturned" in outDoc):
            try:
                outDoc["Ratio"] = outDoc["docsExamined"]/outDoc["nreturned"]
            except Exception:
                pass

        return(outDoc)
    return None

def fmtArray(array):
    arrayShape = "[ "
    scalar = 0;
    parameters = []
    for element in array:
        if isinstance(element,dict):
            if not arrayShape == "[ ":
                arrayShape += ",";
            arrParam = []
            docTxt,subParams = fmtQuery(element)
            arrayShape += docTxt
            for subPar in subParams:
                parameters.append(subPar)
        elif isinstance(element,list):  # Nested Array 
            if not arrayShape == "[ ":
                arrayShape += ",";
            docTxt,subParams = fmtArray(element)
            arrayShape += docTxt
            for subPar in subParams:
                parameters.append(subPar)
        else:
            parameters.append(element)
            scalar += 1             

    if scalar > 0: 
        if not arrayShape == "[ ":
            arrayShape += ","
        if scalar == 1:
            arrayShape += "1"
        else:
            arrayShape += "N"
    arrayShape += " ]"
    return arrayShape,parameters

def fmtQuery(input):
    #print(input)
    parameters = []
    if isinstance(input,str):
        try:
            input,_ = cleanJSON(input, 0)
            #print(input)
            #filter = json_util.loads(input)
            filter = json.loads(input, object_hook=json_util.object_hook)
        except Exception as err:
            try:
                filter = simplejson.loads(input)
            except Exception as err:
                logger.logInfo("===========\n{}\n{}\n===========".format(input,err))
                return input, parameters
    elif not isinstance(input,dict):
        return input, parameters
    else:
        filter = input
    if isinstance(filter,list):  # aggregation pipeline
        if "_$match" in filter[0]:
            filter = filter[0]["_$match"]
        else:
            return input,parameters
    queryShape = "{"
    for key in filter:
        if key[0:2] == "_$":
            fltKey = key[1:]
        else:
            fltKey = key
        if not queryShape == "{": 
            queryShape += ","
        if key.lower() == "$nin":
            hasNIN= True
        if key.lower() == "$ne":
            hasNE= True
        value = filter[key]
        valtxt = None
        if isinstance(value,dict) :
            docTxt,subParams = fmtQuery(value)
            valtxt = fltKey + ": " + docTxt
            for subPar in subParams:
                parameters.append(subPar)

                  
        elif isinstance(value,list):
            docTxt,subParams = fmtArray(value)
            valtxt = fltKey + ": " + docTxt
            for subPar in subParams:
                parameters.append(subPar)
        else:
            parameters.append(value);
            valtxt = fltKey+": 1";
        queryShape += valtxt
    queryShape = queryShape + "}"
    return queryShape,parameters

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
        parser.add_argument('-V', '--version', action='version', version=program_version_message)
        parser.add_argument('--iso8859', dest="isoEncoding", action="store_true", help="Input file is ISO-8859-1 rather than UTF-8")
        parser.add_argument('--URI', dest="URI", metavar='uri', help="MongoDb database URI")
        parser.add_argument('-c', '--coll', dest="coll", metavar='coll', help="Collection to import into")
        parser.add_argument("-s", "--startdate", dest="start", metavar="Start From", help="The Start Date - format YYYY-MM-DD", type=valid_date)
        parser.add_argument(dest="paths", help="paths to folder(s) with source file(s) [default: %(default)s]", metavar="path", nargs='+')

        # Process arguments
        args = parser.parse_args()
        #print(args.URI)
        paths = args.paths
        print(args.start)
        if args.debug:
            logLevel = myLogger.DEBUG
        elif args.verbose:
            logLevel = myLogger.INFO
        else:
            logLevel = myLogger.MESSAGE
        
        logger = myLogger("logParser",file=args.logFile,datafile=args.failFile,severity=logLevel)

            
        logger.logInfo("Got "+paths[0])

        myClient = pymongo.MongoClient(args.URI) 
        mydb = myClient.get_default_database()

        mycol = mydb[args.coll]
        
        newentry = re.compile("^[0-9]{4,4}-[0-9]{2,2}-[0-9]{2,2}T")
        


        
        #f = open("/Users/peterwilliamson/testapp.log", "r")
        #f = open("/Users/peterwilliamson/projects/snapfish/mongodb.log.2", "r")
        for inpath in paths:
            fileEncoding = "utf-8"
            if args.isoEncoding:
                fileEncoding= "ISO-8859-1"
            f = open(inpath, "r",encoding = fileEncoding)
            print(inpath)
            docBuf = []
            
            #Readahead  the first line
            lineBuffer = f.readline().rstrip()  
            linecount = 0
            #loop reading next line, current is in x
            for newline in f:
              linecount += 1
              if (linecount % 10000) == 0:
                  print(linecount)
              # next line is a new entry so process the buffer in x
              if newentry.match(newline):
                  try:
                      ret = procLine(lineBuffer,inpath,linecount)
                  except Exception as progEx:
                    _, exceptionObject, tb  = sys.exc_info()
                    stackSummary = traceback.extract_tb(tb,3)
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
                          docBuf.append(fixDollar(ret))
                  if len(docBuf) >= 1000:
                      try:
                          mycol.insert_many(docBuf,ordered=False)
                      except Exception as err:
                          print(err)

                          print(docBuf)
                      docBuf[:] = []
                      
                  #done with x move the readahead into x 
                  lineBuffer = newline
              else:
                  # multi line log entry append 
                  lineBuffer += newline
            #Process the last line
            try:
                ret = procLine(lineBuffer,inpath,linecount)
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
                if ret != None:
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

if __name__ == "__main__":
    sys.exit(main())    
          
          
