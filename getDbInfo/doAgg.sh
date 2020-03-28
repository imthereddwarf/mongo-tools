echo $1
mongo "$1" <<EOF
db.$3.aggregate([{\$match: {"_id.DB": {\$ne: "all"}}},{ \$project: { _id:1, Server: "\$_id.Server", Collection: {\$concat: ["\$_id.DB", ".", "\$_id.Collection"]}, ranAt: "\$collectedAt", Sizes: { \$objectToArray: "\$indexSizes" },indexStats:1}},{\$out: "$3_stats"}])
db.$3_stats.aggregate([{\$match: {"_id.DB": {\$ne: "all"}}},{\$project: {_id:1, Collection:1, Sizes:1,Server:1}},{\$unwind: "\$Sizes"},{\$group: {_id: {Server: "\$Server",DB: "\$_id.DB", Collection: "\$Collection"}, TotalSize: {\$sum: "\$Sizes.v"}, Count: {\$sum: 1}}},{\$sort: {TotalSize:-1}},{\$out: "$3_ind_ts"}])
db.$3_stats.aggregate([{\$project: {_id: 0, nsid:"\$_id", Collection:1, ranAt:1, indexStats:1, Server:1}},{\$unwind: "\$indexStats"},{\$project: { nsid:1, indVer: "$indexStats.v", ns: { name: {\$concat: ["\$Collection", ".", "\$indexStats.name"]}, Server: "\$Server"},
Unused: {\$cond: [{\$lte: ["\$indexStats.accesses.ops", 0]}, true, false]}, IsId: {\$cond: [{\$eq: ["\$indexStats.name", "_id_"]}, true, false]},
Ops: {\$divide: ["\$indexStats.accesses.ops", {\$divide: [{\$subtract: ["\$ranAt", "\$indexStats.accesses.since"]}, 1000 ]}]},
Days: {\$divide: [{\$subtract: ["\$ranAt", "\$indexStats.accesses.since"]}, 1000*60*60*24 ]}
}},{\$out: "$3_ind_ops"}])
db.$3_stats.aggregate([{\$project: {_id:0, nsid:"\$_id", Collection:1, Sizes:1, Server:1}},{\$unwind: "\$Sizes"},{\$project: { nsid:1, ns: {name: {\$concat: ["\$Collection", ".", "\$Sizes.k"]}, Server: "\$Server"}, index: "\$Sizes.k", size: "\$Sizes.v"}},{\$out: "$3_ind_size"}])
db.$3_ind_size.aggregate([{\$lookup: {from: "$3_ind_ops", localField: "ns", foreignField: "ns", as: "Ops"}},{\$out: "$3_merged"}])
EOF


echo "Server,DB,NS,TotalSize,Count" > $3_indexsizes.csv
mongoexport --uri "$1" -c $3_ind_ts --type csv -f "_id.Server,_id.DB,_id.Collection,TotalSize,Count" --noHeaderLine >> $3_indexsizes.csv
echo "Server,Idx Path,Index,Size,Ops,DB,Collection,Unused,IsId,Days" > $3_merged.csv
mongoexport --uri "$1" -c $3_merged --type csv -f "ns.Server,ns.name,index,size,Ops.0.Ops,nsid.DB,nsid.Collection,Ops.0.Unused,Ops.0.IsId,Ops.0.Days,indVer" --noHeaderLine >> $3_merged.csv
