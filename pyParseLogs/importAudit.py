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
            return None

    if tok == None:
        return None
    if isinstance(tok["ts"], dict):   # simplejson doesn't handle $date
        tmps = '{"t" : ' + str(tok["t"]).replace("'",'"')+ '}'
        tmpd = json.loads(tmps,object_hook=json_util.object_hook)
        d = tmpd["t"]
    else:
        d = tok["ts"]
    if "start" in timeRange and d < timeRange["start"]:
        return None
    if "end" in timeRange and d > timeRange["end"]:
        return True    #nothing more to process here
    
    outDoc = { "type": tok["atype"], "ts": d, "infile": inpath, "lineno": linecount, "shortName": shortName}
    if nodeType != None:
        outDoc["nodeType"] = nodeType
    outDoc['raw'] = tok
    return outDoc

