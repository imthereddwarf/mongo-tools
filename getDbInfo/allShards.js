function allShards(prefix) {
    let isMst = rs.isMaster();
    let clusterType = "";
    if (isMst["msg"] == "isdbgrid") {
	clusterType = "sharded";
	shards = 
    } else if (Array.isArray(isMst["hosts"])) {
	clusterType = "replSet";
    }
    else
	clusterType = "standalone";
    print(clusterType);
}


if (cfgColls.includes("shards")) { use admin; shards = db.adminCommand({ listShards: 1 });     }
