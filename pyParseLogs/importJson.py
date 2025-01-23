import json, re
import simplejson
from bson import json_util

ipport = re.compile("([0-9]*\.[0-9]*\.[0-9]*\.[0-9]*):([0-9]*)")
connection = re.compile("\[conn([0-9]*)\]");

filterLocations = {"update": "q", "count": "query", "find": "filter", "aggregate": "pipeline", "delete": "deletes.q", 
                    "findAndModify": "query", "command": "q", "remove": "q"}

logger = None

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
            cmdType,obj = list(attr["command"].items())[0]
            if cmdType == 'getMore':
                cursor = obj
                outDoc['Object'] = attr["ns"].split(".")[1]
            else:
                outDoc["Object"] = obj
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
                        if isinstance(filter, list) and len(filter) > 0:
                            filter = filter[0]
                    if filter != None:
                        outDoc["filter_shape"],outDoc["filter_params"] = fmtQuery(filter)
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
                        outDoc["filter_shape"],outDoc["filter_params"] = fmtQuery(filter)
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
                        outDoc["filter_shape"],outDoc["filter_params"] = fmtQuery(filter)
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
                        outDoc["filter_shape"],outDoc["filter_params"] = fmtQuery(filter)
                    #del parent[key]
                outDoc["attr"] = cmdData
            else:
                print("Error parsing command: in {} on line {}".format(inpath,linecount))
        else:
            outDoc["Command"] = attr["type"]
            if "ns" in attr:
                outDoc["Object"] = attr["ns"].split(".")[1]

        for key in attr:
            if (key != "type") & (key != "command"):
                outDoc[key] = attr[key]

        if ("filter_shape" not in outDoc) and ("originatingCommand" in outDoc):
            if ("Command" in outDoc) and (outDoc["Command"] == "getMore") and ("filter" in outDoc["originatingCommand"]):
                outDoc["filter_shape"],outDoc["filter_params"] = fmtQuery(outDoc["originatingCommand"]["filter"])
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
                        outDoc["filter_shape"],outDoc["filter_params"] = fmtQuery(filter)
        
        
            

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
    if not isinstance(input,dict):
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
