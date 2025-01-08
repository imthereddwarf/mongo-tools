let skipDB = ["admin","config","test", "local"];
const skipColl = ["system.views"];
sampleSize = 5;
databaseName = "gainwell"
collectionName = "Commits";

//
// Insert a Regex pattern here
//
myPat = /(name|inserted)/i

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


function allCollKeys(pattern,typeName) {
	ns = db.getName() + "." + collectionName + "." + typeName ;
	matches = {};
	try {
	    if (skipColl.indexOf(collection) == -1) {
		db[collection].aggregate([{$match: {'Events.0.Payload.Body._t': typeName}},{$sample: {size: sampleSize}}]).forEach( doc => {
		    let myDoc = doc;
		    for (const key in doc) {
			if (myPat.test(key)) {
			    matches[key] = true;
			}
		    }
		})
	    }
	} catch (error) {
	    for (const em in error) {
		print(em);
	    }
	    print(ns+": "+error.code);
	}
	for (var match in matches) {
	    print(ns+"."+match);
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


