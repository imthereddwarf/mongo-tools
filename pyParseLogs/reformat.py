class reformat:
    
    SHAPE = 1
    TOKENIZE = 2
    REDACT = 3
    
    def __init__(self,mode=None):
        if mode != None:
            self.mode = mode
            self
        else:
            self.mode = self.SHAPE
        return
       
    def process(self,input):
        if self.mode == self.SHAPE:
            return self.fmtQuery(input)
        else:
            return {}
        
    def fmtArray(self,array):
        arrayShape = "[ "
        scalar = 0;
        parameters = []
        for element in array:
            if isinstance(element,dict):
                if not arrayShape == "[ ":
                    arrayShape += ",";
                arrParam = []
                docTxt,subParams = self.fmtQuery(element)
                arrayShape += docTxt
                for subPar in subParams:
                    parameters.append(subPar)
            elif isinstance(element,list):  # Nested Array 
                if not arrayShape == "[ ":
                    arrayShape += ",";
                docTxt,subParams = self.fmtArray(element)
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
    
    def fmtQuery(self,input):
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
                docTxt,subParams = self.fmtQuery(value)
                valtxt = fltKey + ": " + docTxt
                for subPar in subParams:
                    parameters.append(subPar)
    
                      
            elif isinstance(value,list):
                docTxt,subParams = self.fmtArray(value)
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
