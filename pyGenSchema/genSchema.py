
from tkinter import *
 
from tkinter import scrolledtext
from bson.json_util import loads
import json
import tkFileDialog
import tkMessageBox
import sys


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

class validatorDef:
    def __init__(self,fName,fType,parent):
        self.fName = fName
        self.fType = fType
        self.fRequired = []
        self.parent = parent
        self.children = []
        self.desc = ""
        self.typVar = StringVar()
        self.reqVar = IntVar()
        self.descVar = StringVar()
        self.typVar.set(fType)

class VerticalScrolledFrame(Frame):
    """A pure Tkinter scrollable frame that actually works!
    * Use the 'interior' attribute to place widgets inside the scrollable frame
    * Construct and pack/place/grid normally
    * This frame only allows vertical scrolling

    """
    def __init__(self, parent, *args, **kw):
        Frame.__init__(self, parent, *args, **kw)            

        # create a canvas object and a vertical scrollbar for scrolling it
        vscrollbar = Scrollbar(self, orient=VERTICAL)
        vscrollbar.pack(fill=Y, side=RIGHT, expand=FALSE)
        canvas = Canvas(self, bd=0, height=1000, highlightthickness=0,
                        yscrollcommand=vscrollbar.set)
        canvas.pack(side=LEFT, fill=BOTH, expand=TRUE)
        vscrollbar.config(command=canvas.yview)

        # reset the view
        canvas.xview_moveto(0)
        canvas.yview_moveto(0)

        # create a frame inside the canvas which will be scrolled with it
        self.interior = interior = Frame(canvas)
        interior_id = canvas.create_window(0, 0, window=interior,
                                           anchor=NW)

        # track changes to the canvas and frame width and sync them,
        # also updating the scrollbar
        def _configure_interior(event):
            # update the scrollbars to match the size of the inner frame
            size = (interior.winfo_reqwidth(), interior.winfo_reqheight())
            canvas.config(scrollregion="0 0 %s %s" % size)
            if interior.winfo_reqwidth() != canvas.winfo_width():
                # update the canvas's width to fit the inner frame
                canvas.config(width=interior.winfo_reqwidth())
        interior.bind('<Configure>', _configure_interior)

        def _configure_canvas(event):
            if interior.winfo_reqwidth() != canvas.winfo_width():
                # update the inner frame's width to fill the canvas
                canvas.itemconfigure(interior_id, width=canvas.winfo_width())
        canvas.bind('<Configure>', _configure_canvas)


        

class readSchema:
    def __init__(self,master):  
        self.bsonTypeMap = { "Double": "double", "String": "string", "Object": "object", "Array": "array", "Binary": "binData", \
               "Undefined": "undefined", "ObjectID": "objectId", "Boolean": "bool", "Date": "date", "Null": "null", \
                "Regular": "regex", "DBPointer": "dbPointer", "JavaScript": "javascript", "Symbol": "symbol", \
                "JavaScript": "javascriptWithScope", "Int32": "int", "Timestamp": "timestamp", "Long": "long", \
                "Decimal128": "decimal" }   
        
        self.bsonLookup =  {v: k for k, v in self.bsonTypeMap.items()}
        self.valLevels = {"Off": "off","Moderate": "moderate","Strict": "strict"}
        self.valActions = {"Warning": "warn", "Error": "error"}
        self.levelLookup = {v: k for k, v in self.valLevels.items()}
        self.actionLookup = {v: k for k, v in self.valActions.items()}
        self.master = master
        self.frame = Frame(self.master, bg='red')
        self.frame.pack()
        self.fields = []    
        self.maxNameLen = 0
        self.proclevel = 1
        self.top = {}
        
    def doImport(self):
        self.filename = tkFileDialog.askopenfilename(initialdir = "~", title="Select Schema definition")
        print (self.filename)
        with open(self.filename, "r") as sfile:
            data = sfile.read()
        try:
            schemaDesc = loads(data)
        except ValueError as e:
            print(e)  #Expecting property name: line 323 column 5 (char 8304)
            return 
        if "validator" in schemaDesc:
            if "$jsonSchema" in schemaDesc["validator"]:    
                self.editSchema(schemaDesc)
            else:
                tkMessageBox.showwarning("Import Schema", self.filename+" does not contain a JSON Schema.")
        else:
            tkMessageBox.showwarning("Import Schema", self.filename+" does not contain a schema validator.")
            
    def doExport(self):
        self.f= tkFileDialog.asksaveasfile(mode="w", initialdir = "~", title="Select file")
        self.proclevel = 1
        self.schema= {"title": self.schTitle.get(), "description": self.top.descVar.get(), "bsonType": "object", "properties": self.buildSchema(self.top)}
        if len(self.top.fRequired) > 0:
            self.schema["required"] = self.top.fRequired
        print(self.top.fName)
        self.header = {"validator": {"$jsonSchema": self.schema},"validationLevel": self.valLevels[self.schLevel.get()], "validationAction": self.valActions[self.schAction.get()]}
        self.json = json.dumps(self.header,indent=2)
        self.f.write(self.json)
        self.f.close()
        self.importFrame.destroy()
        
    def buildSchema(self,valdef):
        print(self.proclevel,len(valdef.children))
        schema = {}
        for fld in valdef.children:
            print(fld.fName)
            props = {"bsonType": self.bsonTypeMap[fld.typVar.get()]}
            if fld.reqVar.get() > 0:
                valdef.fRequired.append(fld.fName)
            if fld.descVar.get() != "":
                props["description"] = fld.descVar.get()
            if fld.typVar.get() == "Object":
                props["properties"] = self.buildSchema(fld)
                if len(fld.fRequired) > 0:
                    props["required"] = (fld.fRequired)
            schema[fld.fName] =  props         
        return(schema)
    
    def editSchema(self,schemaDef):
        self.schema = schemaDef["validator"]["$jsonSchema"]
        self.importFrame = VerticalScrolledFrame(self.master)
        self.importFrame.pack(fill=X)
        self.schTitle = StringVar()
        self.schLevel = StringVar()
        self.schAction = StringVar()
        if "validationLevel" in schemaDef:
            self.schLevel.set(self.levelLookup[schemaDef["validationLevel"]])
        if "validationAction" in schemaDef:
            self.schAction.set(self.actionLookup[schemaDef["validationAction"]])

        self.top = validatorDef("root",None,None)
        
        self.schTitle.set(self.schema["title"])
        self.top.descVar.set(self.schema["description"])
        
        myFrame = Frame(self.importFrame.interior, bg="cyan")
        myFrame.pack(fill=X)
        
        Label(myFrame, text="Title", bg="cyan").grid(column=0, row=0)
        e = Entry(myFrame, width=80, textvariable = self.schTitle, bg='light cyan')
        e.grid(column=1,row=0) 
        e.config(highlightbackground = "cyan")   
        Label(myFrame, text="ValidationLevel",bg="cyan").grid(column=2, row=0)
        OptionMenu(myFrame,self.schLevel, *self.valLevels).grid(column=3, sticky=EW, row=0)
        Label(myFrame, text="Description", bg="cyan").grid(column=0, row=1)
        e = Entry(myFrame, width=80, textvariable = self.top.descVar, bg='light cyan')
        e.grid(column=1, row=1)
        e.config(highlightbackground = "cyan")   
        Label(myFrame, text="ValidationAction",bg="cyan").grid(column=2, row=1)
        OptionMenu(myFrame,self.schAction, *self.valActions).grid(column=3, sticky=EW, row=1)
        
        myFrame = Frame(self.importFrame.interior, bg="mint cream")
        myFrame.pack(fill=X)

        Label(myFrame, text="Field Name", bg="mint cream",anchor=W).grid(column=0, row=0, columnspan=9, sticky=W)
        Label(myFrame, text="Type",width=25,bg="mint cream",anchor=W).grid(column=10, row=0)
        Label(myFrame, text="Req", width=4, bg="mint cream",anchor=W).grid(column=11, row=0)
        Label(myFrame, text="Description", bg="mint cream",anchor=W).grid(column=12, row=0, sticky=W)

        self.currentRow = 1
        self.myFrame = myFrame

        self.processObject("$jsonSchema",self.schema["properties"],1,self.top)
        
        myFrame = Frame(self.importFrame.interior, bg="cyan")
        myFrame.pack(fill=X)
        self.doneButton = Button(myFrame, text="Save", width=25, command=self.doExport)
        self.doneButton.pack()



    def processObject(self,name, objectDef,level,parent):
        print(level,parent.fName)        
        for fRow in objectDef:
            print(fRow)
            print(objectDef[fRow]["bsonType"])
            myType = self.bsonLookup[objectDef[fRow]["bsonType"]]
            item = validatorDef(fRow,myType,parent)
            if "description" in objectDef[fRow]:
                item.descVar.set(objectDef[fRow]["description"])
            if fRow in parent.fRequired:
                item.reqVar.set(1)
            
            parent.children.append(item)
            if self.currentRow % 2 == 0:
                rowColor = "light cyan"
            else:
                rowColor = 'mint cream'
            print(fRow)
            labelText=  fRow+" ("+objectDef[fRow]["bsonType"]+")"
            if (level > 1):
                indent = 3*(level-1)
                Label(self.myFrame, text="", width=indent, anchor=W, bg=rowColor).grid(column=0,columnspan=level-1, sticky=W, row =self.currentRow)
            else:
                indent = 0
            Label(self.myFrame, text=labelText, anchor=W, width=25-indent, bg=rowColor).grid(column=level-1,columnspan=9-level, sticky=W, row =self.currentRow)
            OptionMenu(self.myFrame,item.typVar, *self.bsonTypeMap).grid(column=10, sticky=EW, row=self.currentRow)
            Checkbutton(self.myFrame,var=item.reqVar, width=4).grid(column=11, sticky=W, row =self.currentRow)
            Entry(self.myFrame, textvariable=item.descVar, width=80).grid(column=12, sticky=W, row =self.currentRow)
            self.currentRow += 1
            if myType == "Object":
                self.processObject(fRow,objectDef[fRow]["properties"],level+1,item)
            print(name+": "+str(level))



                 
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
            fieldsOut.append(schemaField(fldName,level,types,fld["probability"],isUnique))
            if subdoc is not None:
                self.procDocument(level+1,subdoc,fieldsOut)
                
    def doImport(self):
        schemaDef = self.master.clipboard_get()
        schemaDesc = loads(schemaDef)
        self.procDocument(1, schemaDesc["fields"], self.fields)
        self.importFrame = Frame(self.master)
        self.importFrame.pack(fill=X)
        
        currentRow = 1
        for fRow in self.fields:
            if currentRow % 2 == 0:
                rowColor = "light cyan"
            else:
                rowColor = 'mint cream'
            print(fRow.name,not bool(fRow.unique))
            myFrame = Frame(self.importFrame, bg=rowColor)
            myFrame.pack(fill=X)
            labelText=  fRow.name+" ("+str(fRow.percent)+"%)"
            if (fRow.level > 1):
                indent = 3*(fRow.level-1)
                Label(myFrame, text="", width=indent, anchor=W, bg=rowColor).grid(column=0,columnspan=fRow.level-1, sticky=W, row =0)
            else:
                indent = 0
            Label(myFrame, text=labelText, anchor=W, width=self.maxNameLen-indent, bg=rowColor).grid(column=fRow.level-1,columnspan=10-fRow.level, sticky=W, row =0)
            currentCol = 11
            buttonId = 1
            Radiobutton(myFrame, variable=fRow.gridVar, bg=rowColor, text="Ignore", value=0).grid(column=currentCol,row=0,sticky=W)
            currentCol += 1;
            for typ in fRow.types:
                Radiobutton(myFrame, variable=fRow.gridVar, bg=rowColor, text=typ.type+"("+str(typ.percent)+"%)", value=buttonId).grid(column=currentCol,row=0,sticky=W)
                currentCol += 1
                buttonId += 1
            currentRow += 1
        if currentRow % 2 == 0:
            rowColor = "light cyan"
        else:
            rowColor = 'mint cream'
        myFrame = Frame(self.importFrame, bg=rowColor)
        myFrame.pack(fill=X)
        self.doneButton = Button(myFrame, text="Generate", width=25, command=self.doExport)
        self.doneButton.pack()
        
    def doExport(self):
        self.f= tkFileDialog.asksaveasfile(mode="w", initialdir = "~", title="Select file")
        self.schema= [{"title": "Auto Generated Schema", "description": "Fill this here", "bsonType": "object"}, {}]
        self.procLevel = 1
        self.currentField = ["$jsonSchema"]
        self.header = {"validator": {"$jsonSchema": {}},"validationLevel": "moderate", "validationAction": "warn"}
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
        self.header["validator"]["$jsonSchema"] = self.schema[0]
        self.json = json.dumps(self.header)
        self.f.write(self.json)
        self.f.close()
        self.importFrame.destroy()
        editApp = readSchema(self.master)
        editApp.editSchema(self.header)
        
                
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
    print(sys.version)
    window = Tk()
    window.title("MongoDB Schema Generator")
    window.geometry('1024x1024')    

    app = chooseInputSource(window)
    print(app.choice)

        
    window.mainloop()
    
if __name__ == '__main__':
    main()
    