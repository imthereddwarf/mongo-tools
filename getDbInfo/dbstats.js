let server = "unknown";
let tags = null
let skip = ["admin","config","test", "local"];

function serverType() {
    im = db.isMaster();
    if (im.secondary == null) {
	ss = db.serverStatus();
	if (ss.process == "mongos") {
	    server = ss.host;
	    return("Mongos");
	}
	else {
	    server = im.me;
	    tags = im.tags;
	    return("Standalone");
	}
    }
    else {
	server = im.me;
	if (im.secondary) return("Secondary")
	else return("Primary")
    }
}

function allCollStats(server,runTime) {
db.getCollectionInfos({type: "collection"},true).forEach(function(coll) {
   collection = coll.name;
   print('{ _id: {Server: "'+server+'", DB: "' + db + '",Collection: "' + collection + '"},\n   runTime: '+runTime+',\n');
   stats = db[collection].stats();
   print("     NumIndexes: "+stats.nindexes+",");
   print("     totalIndexSize: "+stats.totalIndexSize+",");
   print("     size: "+stats.size+",");
   print("     count: "+stats.count+",");
   print("     avgObjSize: "+stats.avgObjSize+",");
    print("     storageSize: "+stats.storageSize+",");
    print("     freeSpace: "+stats["wiredTiger"]["block-manager"]["file bytes available for reuse"]+",")
    print('     collectedAt: ISODate("'+new Date().toISOString()+'"),');
   print("     indexSizes: ");
    printjson(stats.indexSizes);
    if (stats.hasOwnProperty("shards")) {
	print(",    shardStats: {");
	first = true
	for (shard in stats.shards) {
	    if (first) first = false;
	    else print(",")
	    print('        "'+shard+'": {')
	    print("     NumIndexes: "+stats.shards[shard].nindexes+",");
	    print("     totalIndexSize: "+stats.shards[shard].totalIndexSize+",");
	    print("     size: "+stats.shards[shard].size+",");
	    print("     count: "+stats.shards[shard].count+",");
	    print("     avgObjSize: "+stats.shards[shard].avgObjSize+",");
	    print("     storageSize: "+stats.shards[shard].storageSize+",");
	    print("     freeSpace: "+stats.shards[shard]["wiredTiger"]["block-manager"]["file bytes available for reuse"]+",")
	    print("     indexSizes: ");
	    printjson(stats.shards[shard].indexSizes);
	    print("}");
	}
	print("     }");
    }
   print(",     indexStats: [");
   var indver = db[collection].getIndexes();
   var indspec = {};
    indver.forEach((index) => { indspec[index.name] = index });
    try {
	var objout = db[collection].aggregate([{$indexStats:{}}]);
    }
    catch(err) {
	print("{err: '"+err.code+"'}]}");
	return;
    }
	
    objout.forEach((index) =>  {
	if (!index.hasOwnProperty("spec")) index["spec"] = indspec[index.name]
       //index["v"] = indv[index.name]
       printjson(index);
   if (objout.hasNext()) print(",");
});
print("]}");
});
}



// Switch to admin database and get list of databases.
let stype = serverType();
nodeInfo=rs.isMaster();
if (!nodeInfo.ismaster) rs.secondaryOk();
db = db.getSiblingDB("admin");
const runTime = 'ISODate("'+new Date().toISOString()+'")'
print("--CUT HERE--");
let dbs = db.runCommand({ "listDatabases": 1 }).databases;
print('{ _id: {Server: "server", DB: "all", Collection: "all"},\n    runTime: '+runTime+',\n   Tags: ');
printjson(tags);
print(', Databaselist: ');
printjson(dbs);
print("}");
// Get Oplog stats
if (stype != "Mongos") {
	db = db.getSiblingDB("local");
	let start = db.oplog.rs.find().sort({$natural:1}).limit(1).next();
	let end = db.oplog.rs.find().sort({$natural:-1}).limit(1).next();
	let headroom = (end.ts.t-start.ts.t)/(3600);
	print('{ _id: {Server: "'+server+'", DB: "local" ,Collection: "oplog.rs"},\n   runTime: '+runTime+',\n');
	stats = db["oplog.rs"].stats();
	print("     size: "+stats.size+",");
	print("     maxSize: "+stats.maxSize+",");
	print("     headroom: "+headroom+",");
	print("     avgObjSize: "+stats.avgObjSize+",");
	print("     storageSize: "+stats.storageSize+"}");
}

// Iterate through each database and get its collections.
dbs.forEach(function(database) {
    db = db.getSiblingDB(database.name);
    db.getName();
    if (skip.indexOf(db.getName()) == -1)
	allCollStats(server,runTime);

});


