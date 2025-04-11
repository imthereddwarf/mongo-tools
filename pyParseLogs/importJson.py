import json, re
import simplejson
import datetime
from bson import json_util
from reformat import reformat

ipport = re.compile("([0-9]*\.[0-9]*\.[0-9]*\.[0-9]*):([0-9]*)")
connection = re.compile("\[conn([0-9]*)\]");

filterLocations = {"update": "q", "count": "query", "find": "filter", "aggregate": "pipeline", "delete": "deletes.q", 
                    "findAndModify": "query", "command": "q", "remove": "q"}

logger = None
formater = reformat(reformat.SHAPE)

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



def process(x,inpath,linecount,shortName,timeRange,header,nodeType):
    

    tok = None
    try:
        tok = json.loads(x, object_hook=json_util.object_hook)
    except Exception as err:
        #print(type(err))
        #(cleaned,lastpos) = cleanJSON(snip,0)
        try:
            tok = simplejson.loads(x)
        except Exception as err:
            logger.logInfo("===========\n{}\n{}\n===========".format(x,err))

    if tok == None:
        return None
    if isinstance(tok["t"], dict):   # simplejson doesn't handle $date
        tmps = '{"t" : ' + str(tok["t"]).replace("'",'"')+ '}'
        tmpd = json.loads(tmps,object_hook=json_util.object_hook)
        d = tmpd["t"]
    else:
        d = tok["t"]
    if "start" in timeRange and d < timeRange["start"]:
        return None
    if "end" in timeRange and d > timeRange["end"]:
        return True    #nothing more to process here
    
    outDoc = { "type": tok["c"], "ts": d, "infile": inpath, "lineno": linecount, "shortName": shortName}
    if nodeType != None:
        outDoc["nodeType"] = nodeType
    if "hostname" in header:
        outDoc["host"] = header["hostname"]

    if tok["c"] == "NETWORK":
        if "attr" in tok:
            if "remote" in tok["attr"]:
                ipmatch = ipport.match(tok["attr"]["remote"])
                outDoc['ip_addr'] = ipmatch.group(1)
        if tok["msg"] == "Connection accepted":
            outDoc["Operation"] = "accept"
            outDoc["connection"] = "conn"+str(tok["attr"]["connectionId"])
            outDoc["Count"] = tok["attr"]["connectionCount"]
            return(outDoc)
            #print(outstr)
        elif tok["msg"] == "Connection ended":
            outDoc["Operation"] = "end"
            outDoc["connection"] = tok["ctx"]
            return(outDoc)
        elif tok["msg"] == "client metadata":
            outDoc["Operation"] = "start"
            outDoc["connection"] = tok["attr"]["client"]
            outDoc["connection_info"] = tok["attr"]["doc"]
            return(outDoc)
        elif tok['msg'] == "recv(): message mstLen is invalid.":
            outDoc["thread"] = tok["ctx"] 
            if "msg" in tok:
                outDoc["message"] = tok["msg"]
            if "attr" in tok:
                outDoc["Details"] = tok["attr"]
                if outDoc["Details"]['msgLen'] > 9223372036854775807:
                    outDoc["Details"]['msgLen'] = Decimal128(Decimal(tok['attr']['msgLen']))
            return(outDoc)
        else:
            outDoc["thread"] = tok["ctx"] 
            if "msg" in tok:
                outDoc["message"] = tok["msg"]
            if "attr" in tok:
                outDoc["Details"] = tok["attr"]
            return(outDoc)
        return None
    elif tok["c"] == "CONTROL":
        if tok["msg"] == "Process Details":
            # update the dict passed in, we can't create a new dict
            header.clear()
            header["type"] = "HEADERS"
            header["ts"] =  d
            header[ "infile"] =  inpath
            header[ "lineno"] = linecount
            header[ "shortName"] = shortName
            header[ "isNew"] =  True
            header["Process"] = tok["attr"]
            header["hostname"] = tok["attr"]["host"]
        elif tok["msg"] == "Build Info":
            header["Build"] = tok["attr"]
        elif tok["msg"] == "Operating System":
            header["OS"] = tok["attr"]
        elif tok["msg"] == "Options set by command line":
            header["CmdLine"] = tok["attr"]
        else:
            outDoc["msg"] = tok["msg"]
            return(outDoc)
        return None
    elif tok["c"] == "-" or tok["c"] == "ACCESS":
        outDoc["connection"]  = tok["ctx"]
        outDoc["message"] = tok["msg"]
        if "attr" in tok:
            outDoc["attr"] = tok["attr"]
        return(outDoc)
    elif tok["c"] == "REPL":
        outDoc["thread"] = tok["ctx"] 
        if (tok["msg"] == "Applied op"):
            outDoc["appliedOP"] = list(tok["attr"].items())[0]
            outDoc["Details"] = tok["attr"]
        else:
            outDoc["message"] = tok["msg"]
            if "attr" in tok:
                outDoc["Details"] = tok["attr"]
        return(outDoc)   
    elif tok["c"] == "SHARDING":
        outDoc["thread"] = tok["ctx"] 
        outDoc["message"] = tok["msg"]
        if "attr" in tok:
            outDoc["Details"] = tok["attr"]
        return(outDoc)  
    elif tok["c"] == "SH_REFR":
        outDoc["thread"] = tok["ctx"] 
        outDoc["message"] = tok["msg"]
        if "attr" in tok:
            outDoc["Details"] = tok["attr"]
        return(outDoc)    
    elif tok["c"] == "ELECTION":
        outDoc["thread"] = tok["ctx"] 
        outDoc["message"] = tok["msg"]
        if "attr" in tok:
            outDoc["Details"] = tok["attr"]
        return(outDoc)   
    elif tok["c"] == "TXN":
        outDoc["connection"] = tok["ctx"] 
        if (tok["msg"] == "transaction"):
            outDoc["attr"] = tok["attr"]
        else:
            outDoc["message"] = tok["msg"]
            if "attr" in tok:
                outDoc["Details"] = tok["attr"]
    elif (tok["c"] == "COMMAND") | (tok["c"] == "WRITE"):
        outDoc["connection"] =  tok["ctx"]
        if (tok["msg"] != "Slow query"):
            outDoc["msg"] = tok["msg"]
            if "attr" in tok:
                outDoc["attr"] = tok["attr"]
            return(outDoc)
        attr = tok["attr"]

        if "appName" in attr:
            outDoc["application"] = attr["appName"]
        


        if attr["type"] == "command":
            cmdData = attr["command"]
            if isinstance(cmdData,dict):
                cmdType,obj = list(cmdData.items())[0]
                if cmdType == 'getMore':
                    cursor = obj
                    outDoc['Object'] = attr["ns"].split(".")[1]
                else:
                    outDoc["Object"] = obj
                outDoc["Command"] = cmdType
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
                        if isinstance(filter, list) and len(filter) > 0:
                            filter = filter[0]
                    if filter != None:
                        outDoc["filter_shape"],outDoc["filter_params"] = formater.process(filter)
                    #del parent[key]
                outDoc["attr"] = cmdData
            else:
                print("Error parsing command: in {} on line {}".format(inpath,linecount))
        elif attr["type"] == "getmore":
            cmdType,cursor = list(attr["command"].items())[0]
            orgCmd,obj = list(attr["originatingCommand"].items())[0]
            outDoc["Object"] = obj
            outDoc["Command"] = cmdType
            outDoc["cursor"] = cursor
            if "BatchSize" in attr["command"]:
                outDoc["batchSize"] = attr["command"]["batchSize"]
            cmdData = attr["originatingCommand"]
            if isinstance(cmdData,dict):
                if orgCmd in filterLocations:
                    fltTree = filterLocations[orgCmd].split(".")
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
                        outDoc["filter_shape"],outDoc["filter_params"] = formater.process(filter)
                    #del parent[key]
                outDoc["attr"] = cmdData
            else:
                print("Error parsing command: in {} on line {}".format(inpath,linecount))
        elif attr["type"] == "update":
            cmdType = "update"
            outDoc["Object"] = attr["ns"].split(".")[1]
            outDoc["Command"] = cmdType
            cmdData = attr["command"]
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
                        outDoc["filter_shape"],outDoc["filter_params"] = formater.process(filter)
                    #del parent[key]
                outDoc["attr"] = cmdData
            else:
                print("Error parsing command: in {} on line {}".format(inpath,linecount))
        elif attr["type"] == "remove":
            cmdType = "remove"
            outDoc["Object"] = attr["ns"].split(".")[1]
            outDoc["Command"] = cmdType
            cmdData = attr["command"]
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
                        outDoc["filter_shape"],outDoc["filter_params"] = formater.process(filter)
                    #del parent[key]
                outDoc["attr"] = cmdData
            else:
                print("Error parsing command: in {} on line {}".format(inpath,linecount))
        else:
            outDoc["Command"] = attr["type"]
            if "ns" in attr:
                dbcol = attr["ns"].split(".")
                if len(dbcol) > 1:
                    outDoc["Object"] = dbcol[1]

        for key in attr:
            if (key != "type") & (key != "command"):
                outDoc[key] = attr[key]
                
        if "truncated" in tok:
            outDoc["truncated"] = tok["truncated"]
            
        if "size" in tok:
            outDoc["size"] = tok["size"]

        if ("filter_shape" not in outDoc) and ("originatingCommand" in outDoc):
            if ("Command" in outDoc) and (outDoc["Command"] == "getMore") and ("filter" in outDoc["originatingCommand"]):
                outDoc["filter_shape"],outDoc["filter_params"] = formater.process(outDoc["originatingCommand"]["filter"])
            elif ("Command" in outDoc) and (outDoc["Command"] == "getMore") and ("pipeline" in outDoc["originatingCommand"]):
                aggPipe = outDoc["originatingCommand"]["pipeline"]
                if isinstance(aggPipe,list):
                    filter = None
                    for stage in aggPipe:
                        if "$match" in stage:
                            filter = stage["$match"]
                            break
                        elif ("$addFields" in stage) or ("$set" in stage):
                            continue
                        else:
                            break
                    if filter != None:
                        outDoc["filter_shape"],outDoc["filter_params"] = formater.process(filter)
        
        
            

#
# Now Calculate stuff
#
        if ("docsExamined" in outDoc) & ("nreturned" in outDoc):
            try:
                outDoc["Ratio"] = outDoc["docsExamined"]/outDoc["nreturned"]
            except Exception:
                pass
        waitDoc = None
        if "locks" in outDoc:
            waitDoc = waitTime(outDoc["locks"],"locks")
        if "storage" in outDoc:
            waitDoc = waitTime(outDoc["storage"],"storage", waitDoc)
        if waitDoc != None:
            outDoc["Waits"] = waitDoc
    else:
        connMatch = connection.match(tok["ctx"])
        if connMatch != None:
            outDoc["connection"] = connMatch.group(1)
        if "attr" in tok:
            outDoc["attr"] = tok["attr"]
        if "msg" in tok:
            outDoc["msg"] = tok["msg"]
        if "ctx" in tok:
            outDoc["ctx"] = tok["ctx"]
        return(outDoc)
    if "attr" in outDoc:
        if "lsid" in outDoc['attr']:
            if "id" in outDoc["attr"]["lsid"]:
              outDoc["sessionId"] = outDoc["attr"]["lsid"]["id"]
        if "txnNumber" in outDoc['attr']:
            outDoc["txnNumber"] = outDoc["attr"]["txnNumber"]
        if "parameters" in outDoc['attr']:      
            if "lsid" in outDoc['attr']["parameters"]:
                if "id" in outDoc["attr"]["parameters"]["lsid"]:
                  outDoc["sessionId"] = outDoc["attr"]["parameters"]["lsid"]["id"]
            if "txnNumber" in outDoc['attr']["parameters"]:
                outDoc["txnNumber"] = outDoc["attr"]["parameters"]["txnNumber"]
    if "durationMillis" in outDoc:
        if not isinstance(outDoc["ts"], datetime.datetime):
            print(outDoc["ts"])
        else:
            outDoc["startTS"] = outDoc["ts"] - datetime.timedelta(milliseconds=outDoc["durationMillis"])
    return outDoc

