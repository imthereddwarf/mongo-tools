
import re
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


connection = re.compile("\[conn([0-9]*)\]");
ipport = re.compile("([0-9]*\.[0-9]*\.[0-9]*\.[0-9]*):([0-9]*)")
logKey = re.compile("^[a-zA-Z]*:$")
embedcolon = re.compile("^:({[ }]|[0-9]* )")
embedcolonhex = re.compile("^:({|[0-9A-Fa-f]*) ")
floating = re.compile("^[+-]?[0-9.]+[eE][+-]?[0-9.]+")
digits = re.compile("([0-9]+)")
exTime = re.compile("([0-9]+)ms")
exRange = re.compile("([0-9]+)-([0-9]*)")
exModule = re.compile("^\[([a-zA-Z-]*)(-?[0-9]*)?]$|^\[conn([0-9]*)]$")

notEscapedDouble  = re.compile("[^\\\\]\"")
notEscapedSingle  = re.compile("[^\\\\]\'")

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

filterLocations = {"update": "q", "count": "query", "find": "filter", "aggregate": "pipeline", "delete": "deletes.q", 
                    "findAndModify": "query", "command": "q", "remove": "q"}
               
uKeys = ["multi:","projec","shardV"]


keyTypes = {}
cmdTypes = {}
strTypes = {}
isMQL = {"\"filter\":": 1, "\"q\":":1, "\"u\":":1, "\"pipeline\":": 1, "\"sort\":": 1, "\"query\":": 1, "\"planSummary\":": 1, "\"appliedOp\":": 1,
         "\"originatingCommand\":\"pipeline\"": 1,"\"originatingCommand\":\"filter\"": 1}

logger = None

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
    
def getShardRange(tok,start,jsonObj):
    outDoc = {}
    if tok[start].startswith("[JSON"):
        outDoc['min'] = jsonObj.getJsonVal(tok[start][1:-1])
    if tok[start+1].startswith("JSON"):
        outDoc['max'] = jsonObj.getJsonVal(tok[start+1][:-1])
    return(outDoc)


def waitTime(indoc, root, previous = None):

    isWait,time,names = gotWaiting(indoc,0, False, [], root)
    if isWait:
        if previous is None:
           results = {'totWaitTimeMicros': time, 'locksWaited': names}
        else:
            results['totWaitTimeMicros'] += time
            results['locksWaited'].append(names)
        return(results)
    else:
        return(previous)

def gotWaiting(indoc, time, isWait, names, path):
    keyNames = ["timeReadingMicros","timeWritingMicros","timeWaitingMicros"]

    for keyName in indoc:
        if keyName in keyNames:
            if isinstance(indoc[keyName],dict):
                for lockName in indoc[keyName]:
                    isWait = True    
                    time += indoc[keyName][lockName]
                    names.append(path+"."+lockName+"."+keyName)
            else:
                isWait = True    
                time += indoc[keyName]
                names.append(path+"."+keyName)
        elif isinstance(indoc[keyName],dict):
            nIsWait,nTime,nNames = gotWaiting(indoc[keyName],time, isWait, names, path+"."+keyName)
            if nIsWait:
                isWait = True
                time += nTime
    return(isWait,time, names)

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



def process(x,inpath,linecount,shortName,timeRange,nodeType):
    
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
    
    d = datetime.datetime.strptime(tok[0][0:23], "%Y-%m-%dT%H:%M:%S.%f")
    if "start" in timeRange and d < timeRange["start"]:
        return None
    if "end" in timeRange and d > timeRange["end"]:
        return True  #all done in this file
    
    outDoc = { "type": tok[2], "ts": d, "infile": inpath, "lineno": linecount, "shortName": shortName}
    if nodeType != None:
        outDoc["nodeType"] = nodeType

    
    if tok[2] == "NETWORK":
        if tok[4] == "connection":
            if tok[5] == "accepted":
                outDoc["Operation"] = "Accept"
                outDoc['connection'] = tok[8][1:]
                ipmatch = ipport.match(tok[7])
                outDoc['ip_addr'] = ipmatch.group(1)+'"}'
                return(outDoc)
            else:
                return None
            #print(outstr)
        elif tok[4] == "end":
            outDoc["Operation"] = "end"
            connMatch = connection.match(tok[3])
            outDoc["connection"] = connMatch.group(1)
            ipmatch = ipport.match(tok[6])
            outDoc["ip_addr:"] = ipmatch.group(1) 
            return(outDoc)
        elif tok[4] == "received":
            outDoc["Operation"] = "start"
            connMatch = connection.match(tok[3])
            outDoc["connection"] = connMatch.group(1)
            ipmatch = ipport.match(tok[8])
            outDoc["ip_addr:"] = ipmatch.group(1) 
            outDoc["connection_info"] = jsonObj.getJsonVal(0)
            return(outDoc)
        return None
    elif tok[2] == "SHARDING":
        modMatch = exModule.match(tok[3])
        if modMatch is None:
            outDoc['Module'] = tok[3]
        else:
            if not(modMatch.group(3) == None):
                outDoc["Connection"] = modMatch.group(3)
            else:
                outDoc["Module"] = modMatch.group(1)
        if tok[4] == "moveChunk":
            outDoc['shardOp'] = "status"
            if tok[8].startswith("JSON"):
                outDoc['chunk'] = jsonObj.getJsonVal(tok[8])
            outDoc['memUsed'] = int(tok[11])
            outDoc['docsLeft'] = int(tok[11])
            return(outDoc)
            #print(outstr)
        elif tok[4] == "request" and tok[5] == "split":
            outDoc['shardOp'] = "spltLookup"
            if tok[11].startswith("JSON"):
                outDoc['origChunk'] = jsonObj.getJsonVal(tok[11])
            if tok[13].startswith("JSON"):
                outDoc['newChunk'] = jsonObj.getJsonVal(tok[13])
            outDoc['collection'] = tok[10]
            return(outDoc)
            #print(outstr)
        elif tok[4] == "Updating" and tok[5] == "metadata":
            outDoc['shardOp'] = "updMeta"
            outDoc['collection'] = tok[8]
            outDoc['old'] = {'collection': tok[12], 'shard': tok[15]}
            outDoc['new'] = {'collection': tok[19], 'shard': tok[22]}
            return(outDoc)
        elif tok[4] == "Migration" and tok[5] == "succeeded":
            outDoc['shardOp'] = "succeeded"
            outDoc['new'] = {'collection': tok[11]}
            return(outDoc)
        elif tok[4] == "Migration" and tok[5] == "successfully" and tok[6] == "entered" and tok[7] == "critical": 
            outDoc['shardOp'] = "entered critical"
            outDoc['msg'] = "Migration successfully entered critical section"
            return(outDoc)
        elif tok[4] == "about" and tok[5] == "to" and tok[6] == "log" and tok[7] == "metadata":
            outDoc['shardOp'] = "log"
            if tok[11].startswith("JSON"):
                outDoc['info'] = jsonObj.getJsonVal(tok[11])
            return(outDoc)
        elif tok[4] == "autosplitted":
            outDoc['shardOp'] = "autosplit"
            outDoc['collection'] = tok[5]
            outDoc['range'] = getShardRange(tok,11,jsonObj)
            outDoc['shard'] = tok[8]
            outDoc['lastmod'] = tok[10][:-1]
            outDoc['nParts'] = int(tok[14])
            outDoc['maxChunkSize'] = int(tok[17][:-1])
            return(outDoc)
        elif tok[4] == "Finding" and tok[5] == "the":
            outDoc['shardOp'] = "findAuto"
            outDoc['collection'] =  tok[10]
            if tok[13].startswith("JSON"):
                outDoc['shardKey'] = jsonObj.getJsonVal(tok[13])
            outDoc['numSlits'] = tok[16]
            tMatch = exTime.search(tok[19])
            if (tMatch):
                outDoc['time'] = int(tMatch.group(1))
            return(outDoc)
        elif tok[4] == "No" and tok[5] == "documents":
            outDoc['shardOp'] = "noDoc"
            outDoc['collection'] =  tok[10]
            outDoc['range'] = getShardRange(tok,12,jsonObj)
            return(outDoc)
        elif tok[4] == "Waiting" and tok[5] == "for":
            outDoc['shardOp'] = "waitMaj"
            outDoc['collection'] =  tok[12]
            return(outDoc)
        elif tok[4] == "migrate" and tok[5] == "commit":
            outDoc['shardOp'] = "migComm"
            outDoc['collection'] =  tok[11]
            outDoc['range'] = getShardRange(tok,12,jsonObj)
            return(outDoc)
        elif tok[4] == "Finished" and tok[5] == "deleting":
            if tok[6] == "documents":
                outDoc['shardOp'] = "finDel"
                outDoc['collection'] =  tok[8]
                outDoc['range'] = getShardRange(tok,10,jsonObj)
            else:
                outDoc['shardOp'] = "finDel"
                outDoc['collection'] =  tok[6]
                outDoc['range'] = getShardRange(tok,8,jsonObj)
            return(outDoc)
        elif tok[4] == "Deferring" and tok[5] == "deletion":
            outDoc['shardOp'] = "findAuto"
            outDoc['collection'] =  tok[7]
            outDoc['range'] = getShardRange(tok,9,jsonObj)
            return(outDoc)
        elif tok[4] == "Starting" and tok[5] == "chunk":
            outDoc['shardOp'] = "startMig"
            outDoc['collection'] =  tok[8]
            outDoc['range'] = getShardRange(tok,9,jsonObj)
            return(outDoc)
        elif tok[4] == "Queries" and tok[5] == "possibly":
            outDoc['shardOp'] = "queryPoss"
            outDoc['collection'] =  tok[8]
            return(outDoc)
        elif tok[4] == "Waiting" and tok[5] == "for":
            outDoc['shardOp'] = "waitRepl"
            return(outDoc)
        elif tok[4] == "Scheduling" and tok[5] == "deferred":
            outDoc['shardOp'] = "schedDef"
            outDoc['collection'] =  tok[8]
            outDoc['range'] = getShardRange(tok,10,jsonObj)
            return(outDoc)
        elif tok[4] == "Deletion" and tok[5] == "of":
            outDoc['shardOp'] = "delSched"
            outDoc['collection'] =  tok[6]
            outDoc['range'] = getShardRange(tok,8,jsonObj)
            return(outDoc)
        elif tok[4] == "Leaving" and tok[5] == "cleanup":
            outDoc['shardOp'] = "findAuto"
            outDoc['collection'] =  tok[7]
            outDoc['range'] = getShardRange(tok,9,jsonObj)
            return(outDoc)
        elif tok[4] == "Chunk" and tok[5] == "data":
            outDoc['shardOp'] = "repSucc"
            return(outDoc)
        elif tok[4] == "Scheduling" and tok[5] == "deletion":
            outDoc['shardOp'] = "schedDel"
            outDoc['collection'] =  tok[10]
            outDoc['range'] = getShardRange(tok,12,jsonObj)
            return(outDoc)
        elif tok[4] == "Starting" and tok[5] == "receiving":
            outDoc['shardOp'] = "startRec"
            outDoc['collection'] =  tok[16]
            range = {}
            if tok[11].startswith("JSON"):
                range['min'] = jsonObj.getJsonVal(tok[11])
            if tok[13].startswith("JSON"):
                range['max'] = jsonObj.getJsonVal(tok[13])
            outDoc['chunk'] = range
            outDoc['epoch'] =  ObjectId(tok[21])
            outDoc['source'] = tok[18]
            return(outDoc)
        elif tok[4] == "Updating" and tok[5] == "config":
            outDoc['shardOp'] = "updConfigSet"
            outDoc['cluster'] =  tok[10]
            return(outDoc)
        else:
            outDoc['shardOp'] = tok[4] + tok[5] + tok[6]
            outDoc['line'] = newstr
            print(tok)
        return None
    elif tok[2] == "CONTROL":
        outstr = ""
        #if (tok[4] == "db") & (tok[5] == "version"):
        #    outstr = "{ type: \"CONTROL\", version: \""+tok[6]+"\"}"
        return None
    elif (tok[2] == "ACCESS"):
        connMatch = connection.match(tok[3])
        if connMatch != None:
            outDoc["connection"] =  connMatch.group(1)
        outDoc["user"] = tok[8]
        return(outDoc)
    elif tok[2] == "REPL":
        outDoc["thread"] = tok[3] 
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
            logger.logDebug(tok[0])
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
        if ( c== "."):
            logger.logDebug(outstring)
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
 #           if token[-10:] == "appMessage":
 #               print(token[-10:])
            if saveAsString != 0:  # were saving JSON as a String
                if (c == instring):
                    #print(token)
                    token += "\\"+ c   # escape the quotes
 #                   print(token[-4:])
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
#                            print(token)
                            instring = '\0'
                    elif saveAsString == 1:                    # end of JSON
                        saveAsString = 0
                        token += c + '"'
                        instring = '\0'
                    else:
                        token += c
                elif (c < ' ') | (c > '~') : # unprintable
#                    print(hex(ord(c)))
#                    print("{0:#0{1}x}".format(ord(c),6))
                    token += "{0:#0{1}x}".format(ord(c),6).replace("0x","\\u")
                else:
                    token += c
                    
            elif c == instring:
                instring = '\0';  # string closed
                token += '"';
                logger.logDebug(token)
            elif (c < ' ') | (c > '~') : # unprintable
#                print(hex(ord(c)))
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
#                print(token)
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
            logger.logDebug(token)
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
        elif isinstance(value,str):
            if value.startswith("new"):
                if value.startswith("newDate"):
                    epoch = int(value[8:-4])
                    try:
                        dateval = datetime.datetime.fromtimestamp(epoch,pytz.utc)
                    except ValueError as e:
                        dateval = datetime.datetime(9999,12,31,23,59,59)
                    parameters.append(dateval)
            else:
                parameters.append(value);
            valtxt = fltKey+": 1";
        else:
            parameters.append(value);
            valtxt = fltKey+": 1";
        queryShape += valtxt
    queryShape = queryShape + "}"
    return queryShape,parameters

   