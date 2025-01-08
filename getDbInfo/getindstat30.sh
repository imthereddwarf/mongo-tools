if [ -z $3 ]
then
    echo "usage: getindstat connection_string alias outfile [password]"
    exit 1
fi

if [ -z $4 ]
then
    authCmd=""
else
    authCmd="db.auth('admin','$3');"
fi
sourcedir=`dirname $0`
labeltext="Start Here"
    
mongo $1 <<EOF >$3 2>/dev/null
use admin;
$authCmd
rs.slaveOk();
load("${sourcedir}/allCollStats30.js");
let skip = ["admin","config","test", "local"];
// Switch to admin database and get list of databases.
nodeInfo=rs.isMaster();
db = db.getSiblingDB("admin");
print("$labeltext");
let dbs = db.runCommand({ "listDatabases": 1 }).databases;
print('{ _id: {Server: "$2", DB: "all", Collection: "all"},\n    runTime: ISODate("'+new Date().toISOString()+'"),\n   Tags: ');
printjson(nodeInfo.tags);
print(', Databaselist: ');
printjson(dbs);
print("}");
// Iterate through each database and get its collections.
dbs.forEach(function(database) {
    db = db.getSiblingDB(database.name);
    db.getName();
    if (skip.indexOf(db.getName()) == -1)
    allCollStats("$2");

});
exit
EOF

ed $3 <<EOF >/dev/null
1,/$labeltext/d
p
$
d
p
wq
EOF
echo Import $3 with mongoimport
