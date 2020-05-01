
from tkinter import *
 
from tkinter import scrolledtext
from bson.json_util import loads
import json
import tkFileDialog

class schemaField:
    def __init__(self,name,level,fieldTypes,frequency,isUnique):
        self.name = name
        self.level = level
        self.types = fieldTypes
        self.unique = isUnique
        self.percent = frequency*100
        self.gridVar = IntVar()
        
class typeFrequency:
    def __init__(self,type,percentage):
        self.type = type
        self.percent = percentage

class readSchema:
    def __init__(self,master):      
        self.master = master
        self.frame = Frame(self.master, bg='red')
        self.frame.pack()
        self.fields = []    
        self.maxNameLen = 0
        
    def doImport(self):
        self.filename = tkFileDialog.askopenfilename(initialdir = "~", title="Select Schema definition")
        print (self.filename)
        with open(self.filename, "r") as file:
            data = file.read()
        try:
            schemaDesc = loads(data)
        except ValueError as e:
            print(e)  #Expecting property name: line 323 column 5 (char 8304)
            return 
        if "validator" in schemaDesc:
            try:
                core = schemaDesc["validator"]["$jsonSchema"]
            except KeyError as e:
                print(e+ "missing from input schema")
            else:
                print(core["title"])
        
             
class importFromCompass:


    def __init__(self, master): 
        self.bsonTypeMap = { "Double": "double", "String": "string", "Object": "object", "Array": "array", "Binary": "binData", \
               "Undefined": "undefined", "ObjectID": "objectId", "Boolean": "bool", "Date": "date", "Null": "null", \
                "Regular": "regex", "DBPointer": "dbPointer", "JavaScript": "javascript", "Symbol": "symbol", \
                "JavaScript": "javascriptWithScope", "Int32": "int", "Timestamp": "timestamp", "Long": "long", \
                "Decimal128": "decimal" }
        self.master = master
        self.frame = Frame(self.master, bg='red')
        self.frame.pack()
        self.fields = []    
        self.maxNameLen = 0
         
    def procDocument(self,level,schema,fieldsOut):
        for fld in schema:
            freq = fld["count"]
            indent = "-> " * (level-1)
            types = []
            subdoc = None
        
            isUnique = fld["has_duplicates"]  #save here because an array doesn't have this property
            fldName = fld["name"]
            nameLen = len(indent+fldName)+7 # Include (100%) suffix
            if self.maxNameLen < nameLen:
                self.maxNameLen = nameLen  
            if fld["types"][0]["name"] == "Array":
                fld = fld["types"][0]
            for typ in fld["types"]:
                types.append(typeFrequency(typ["bsonType"],typ["count"]*100/freq))
                if typ["bsonType"] == "Document":
                    subdoc = typ["fields"]
            fieldsOut.append(schemaField(indent+fldName,level,types,fld["probability"],isUnique))
            if subdoc is not None:
                self.procDocument(level+1,subdoc,fieldsOut)
                
    def doImport(self):
        schemaDef = self.master.clipboard_get()
        schemaDesc = loads(schemaDef)
        self.procDocument(1, schemaDesc["fields"], self.fields)
        
        currentRow = 1
        for fRow in self.fields:
            if currentRow % 2 == 0:
                rowColor = "light cyan"
            else:
                rowColor = 'mint cream'
            print(fRow.name,not bool(fRow.unique))
            myFrame = Frame(self.master, bg=rowColor)
            myFrame.pack(fill=X)
            labelText=  fRow.name+" ("+str(fRow.percent)+"%)"
            Label(myFrame, text=labelText, anchor=W, width=self.maxNameLen, bg=rowColor).grid(column=0, sticky=W, row =0)
            currentCol = 1
            for typ in fRow.types:
                Radiobutton(myFrame, variable=fRow.gridVar, bg=rowColor, text=typ.type+"("+str(typ.percent)+"%)", value=currentCol).grid(column=currentCol,row=0,sticky=W)
                currentCol += 1
            currentRow += 1
        if currentRow % 2 == 0:
            rowColor = "light cyan"
        else:
            rowColor = 'mint cream'
        myFrame = Frame(self.master, bg=rowColor)
        myFrame.pack(fill=X)
        self.doneButton = Button(myFrame, text="Generate", width=25, command=self.doExport)
        self.doneButton.pack()
        
    def doExport(self):
        self.f= tkFileDialog.asksaveasfile(mode="w", initialdir = "~", title="Select file")
        self.schema= [{"title": "Auto Generated Schema", "description": "Fill this here", "bsonType": "object"}, {}]
        self.procLevel = 1
        self.currentField = ["$jsonSchema"]
        for field in self.fields:
            if field.level < self.procLevel: # End of sub document
                for i in range(self.procLevel,field.level,-1):
                    self.schema[i-1][self.currentField[i-1]] = {"bsonType" : "object", "properties": self.schema[i]}
                    self.schema.pop(i)
                    self.currentField.pop(i-1)
                self.procLevel = field.level
            if field.level == self.procLevel: #field at our current level
                print(field.name,field.gridVar.get())
                if field.gridVar.get() > 0:
                    myType = field.types[field.gridVar.get()-1].type
                    if (myType == "Document"):  #include the sub document
                        self.currentField.append(field.name)
                        self.schema.append({})
                        self.procLevel += 1;
                    else:
                        self.schema[self.procLevel][field.name] =  {"bsonType": self.bsonTypeMap[myType]}
            # else skip as the containing object was not selected
        if self.procLevel > 1: # Trailing sub document
            for i in range(self.procLevel,1,-1):
                self.schema[i-1][self.currentField[i-1]] = {"bsonType" : "object", "properties": self.schema[i]}
        self.schema[0]["properties"] = self.schema[1]
        self.json = json.dumps(self.schema[0])
        self.f.write(self.json)
        self.f.close()
        
                
class chooseInputSource:
    def __init__(self,master):
        self.master = master     
        self.choice = 0
        self.frame = Frame(self.master, bg="green")
        self.button1 = Button(self.frame, text = "Paste", width = 25, command=self.setPaste )
        self.button1.pack(side=LEFT)
        self.button2 = Button(self.frame, text = "Read", width = 25, command=self.setFile )
        self.button2.pack(side=LEFT)
        self.frame.pack()
        
    def setPaste(self):
        self.choice=1
        importApp = importFromCompass(self.master)
        importApp.doImport()
        
    def setFile(self):
        self.choice=2
        importApp = readSchema(self.master)
        importApp.doImport()
        
        
        

    
def main():    
    window = Tk()
    window.title("MongoDB Schema Generator")
    #window.geometry('1024x1024')    
    app = chooseInputSource(window)
    print(app.choice)

        
    window.mainloop()
    
if __name__ == '__main__':
    main()