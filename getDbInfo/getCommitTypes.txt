let skipDB = ["admin","config","test", "local"];
const skipColl = ["system.views"];
sampleSize = 5;
databaseName = "gainwell"
collectionName = "Commits";

//
// Insert a Regex pattern here
//
myPat = /(name|id)/i

getTypesPipeline = [{$match: {
 Events: {
  $type: 4
 }
}}, {$group: {
 _id: '$Events.Payload.Body._t',
 count: {
  $sum: 1
 }
}}, {$project: {
 type: {
  $arrayElemAt: [
   '$_id',
   0
  ]
 },
 count: 1,
 _id: 0
}}]

function getDocKeys(pattern,doc,prefix) {
    let matches = {};
    let myprefix = ""
    if (prefix != null) {
	myprefix = prefix + ".";
    }
    let myDoc = doc;
    for (const key in myDoc) {
	myKey = key;
	if (myPat.test(key)) {
	    matches[myprefix+key] = true;
	}
	let submatch = {};
	if (typeof doc[key] === 'array') {
	    for (const elem in doc[key])
		submatch = getDocKeys(pattern,elem,myprefix+"$");
	}
	if ((typeof doc[key] === 'object') && ! ( doc[key] instanceof ObjectId)){
	    submatch = getDocKeys(pattern,doc[key],myprefix+key);
	}
	if (Object.keys(submatch).length > 0) {
	    for (const subKey in submatch)
		matches[subKey] = 1;
	}
			
    }
    return(matches);
}
    

function allCollKeys(pattern,typeName) {
    ns = db.getName() + "." + collectionName + "." + typeName ;
    matches = {};
   // try {
	db[collectionName].aggregate([{$match: {'Events.0.Payload.Body._t': typeName}},{$sample: {size: sampleSize}}]).forEach( doc => {
	    let myDoc = doc;
	    matches = getDocKeys(pattern,doc,null);
	})
    //} catch (error) {
//	for (const em in error) {
//	    print(em);
//	}
//	print(ns+": "+error.code);
  //  }
    if (Object.keys(matches).length > 0) {
	for (var match in matches) {
	    print(ns+": "+match);
	}
    } else {
	print(ns+": No Matches");
    }
}



// Switch to admin database and get list of databases.
db = db.getSiblingDB(databaseName);
print("--CUT HERE--");
//
// get the comit types
//
types = db[collectionName].aggregate(getTypesPipeline);
// Iterate through each database and get its collections.
types.forEach(function(typeDoc) {
    allCollKeys(myPat,typeDoc.type);
});


