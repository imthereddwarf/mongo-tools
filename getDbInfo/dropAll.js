function dropAllall(server) {
let skip = ["admin","config","test", "local"];
db = db.getSiblingDB("admin");
let dbs = db.runCommand({ "listDatabases": 1 }).databases;
// Iterate through each database and get its collections.
dbs.forEach(function(database) {
    db = db.getSiblingDB(database.name);
    if (skip.indexOf(db.getName()) == -1)
	db.dropDatabase();
}
