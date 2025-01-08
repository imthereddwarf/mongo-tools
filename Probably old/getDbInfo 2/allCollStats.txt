function allCollStats(server) {
db.getCollectionNames().forEach(function(collection) {
   print('{ _id: {Server: "'+server+'", DB: "' + db + '",Collection: "' + collection + '"},');
   stats = db[collection].stats();
   print("     NumIndexes: "+stats.nindexes+",");
   print("     totalIndexSize: "+stats.totalIndexSize+",");
   print("     size: "+stats.size+",");
   print("     count: "+stats.count+",");
   print("     avgObjSize: "+stats.avgObjSize+",");
   print("     storageSize: "+stats.storageSize+",");
   print("     indexSizes: ");
   printjson(stats.indexSizes);
   print(",     indexStats: [");
   var objout = db[collection].aggregate([{$indexStats:{}}]);
   objout.forEach((index) =>  {
   printjson(index);
   if (objout.hasNext()) print(",");
});
print("]}");
});
}
