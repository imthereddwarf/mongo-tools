import json, re
import simplejson
import datetime
from bson import json_util
from reformat import reformat
from test import _test_embed_set_config

ipport = re.compile("([0-9]*\.[0-9]*\.[0-9]*\.[0-9]*):([0-9]*)")
connection = re.compile("\[conn([0-9]*)\]");

filterLocations = {"update": "q", "count": "query", "find": "filter", "aggregate": "pipeline", "delete": "deletes.q", 
                    "findAndModify": "query", "command": "q", "remove": "q"}

logger = None
isconfig = re.compile("^ConfCallResponse")
isConnect = re.compile("^Monitor thread successfully connected")
isClient = re.compile("^MongoClient with metadata")

splitConfig = re.compile("(id|version|mongoDbUri|mongosUri|mongoDbClusterUri|createDate|lastUpdateDate|searchIndexes|vectorIndexes|analyzerDefinitions|blobstoreParams|embeddingServiceConfigs)=(?:Optional\[)?(.+?(?=]?, (id|version|mongoDbUri|mongosUri|mongoDbClusterUri|createDate|lastUpdateDate|searchIndexes|vectorIndexes|analyzerDefinitions|blobstoreParams)))")


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
        d = datetime.datetime.strptime(tok["t"][0:23], "%Y-%m-%dT%H:%M:%S.%f")
    if "start" in timeRange and d < timeRange["start"]:
        return None
    if "end" in timeRange and d > timeRange["end"]:
        return True    #nothing more to process here
    
    outDoc = { "type": tok["svc"], "ts": d, "infile": inpath, "lineno": linecount, "shortName": shortName, "module": tok["n"], "severity": tok["s"]}
    if nodeType != None:
        outDoc["nodeType"] = nodeType
    if "hostname" in header:
        outDoc["host"] = header["hostname"]

    if "msg" in tok:
        outDoc["msg"] = tok['msg']
    if "attr" in tok:
        outDoc["attr"] = tok["attr"]
    if isconfig.match(tok["msg"]):
        config = {}
        for token in  splitConfig.finditer(tok["msg"]):
            config[token.group(1)] = token.group(2)
        outDoc["config"] = config
        outDoc["msg"] = "ConfCallResponse"

    outDoc["raw"] = tok

        

    return outDoc

