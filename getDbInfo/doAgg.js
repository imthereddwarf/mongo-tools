function aggStats(server,dropOnly = false) {
    let statsColl = server.concat("_stats"); //remove header and make sizes an array
    let tsColl = server.concat("_ind_ts"); //unwind szies and group by db:collection and get count and total size
    let opsColl = server.concat("_ind_ops"); //Calulate Ops
    let sizeColl = server.concat("_ind_size"); //Unwind size info and project
    let mergedColl = server.concat("_merged"); //merge size and ops
    //
    // Clean
    //
    db.getCollection(statsColl).drop();
    db.getCollection(tsColl).drop();
    db.getCollection(opsColl).drop();
    db.getCollection(sizeColl).drop();
    db.getCollection(mergedColl).drop();
    if (dropOnly) {
	print("Tables dropped");
	return;
    }
    //
    // Do agg
    //
    db.getCollection(server).aggregate([{$match: {"_id.DB": {$ne: "all"}}},{ $project: { _id:1, Server: "$_id.Server", Collection: {$concat: ["$_id.DB", ".", "$_id.Collection"]}, ranAt: "$collectedAt", Sizes: { $objectToArray: "$indexSizes" },indexStats:1}},{$out: statsColl}]);
    db.getCollection(statsColl).aggregate([{$match: {"_id.DB": {$ne: "all"}}},
					   {$project: {_id:1, Collection:1, Sizes:1,Server:1}},
					   {$unwind: "$Sizes"},
					   {$group: {_id: {Server: "$Server",DB: "$_id.DB", Collection: "$Collection"}, TotalSize: {$sum: "$Sizes.v"}, Count: {$sum: 1}}},
					   {$sort: {TotalSize:-1}},
					   {$out: tsColl}],{allowDiskUse:true});
    db.getCollection(statsColl).aggregate([{$project: {_id: 0, nsid:"$_id", Collection:1, ranAt:1, indexStats:1, Server:1}},
					   {$unwind: "$indexStats"},
					   {$project: { nsid:1, indVer: "$indexStats.v", ns: { name: {$concat: ["$Collection", ".", "$indexStats.name"]}, Server: "$Server"},
							Unused: {$cond: [{$lte: ["$indexStats.accesses.ops", 0]}, 0, 1]}, IsId: {$cond: [{$eq: ["$indexStats.name", "_id_"]}, true, false]},
							Ops: {$divide: ["$indexStats.accesses.ops", {$divide: [{$subtract: ["$ranAt", "$indexStats.accesses.since"]}, 1000 ]}]},
							Days: {$divide: [{$subtract: ["$ranAt", "$indexStats.accesses.since"]}, 1000*60*60*24 ]}
						      }},
					   {$group: {_id: "$ns.name", nsid: {$first:  "$nsid"}, indVer: {$addToSet: "$indVer"}, hosts: {$addToSet: "$ns.Host"}, Unused: {$sum: "$Unused"}, IsId: {$first: "$IsId"}, Ops: {$avg: "$Ops"}, Days: {$min: "$Days"}, nShards: {$sum: 1}}},
					   {$project: {_id:0, nsid:1, indVer:1, "ns.name": "$_id", "ns.Server": "$nsid.Server", Unused: {$cond: [{$lte: ["$Unused", 0]}, true, false]},IsId:1, Ops:1, Days:1, nShards:1 }},
					   {$out: opsColl}],{allowDiskUse:true})
    db.getCollection(statsColl).aggregate([{$project: {_id:0, nsid:"$_id", Collection:1, Sizes:1, Server:1}},
					   {$unwind: "$Sizes"},
					   {$project: { nsid:1, ns: {name: {$concat: ["$Collection", ".", "$Sizes.k"]}, Server: "$Server"}, index: "$Sizes.k", size: "$Sizes.v"}},
					   {$out: sizeColl}],{allowDiskUse:true})
    db.getCollection(opsColl).createIndex({ns:1})
    db.getCollection(sizeColl).aggregate([{$lookup: {from: opsColl, localField: "ns", foreignField: "ns", as: "Ops"}},{$out: mergedColl}],{allowDiskUse:true})
}
