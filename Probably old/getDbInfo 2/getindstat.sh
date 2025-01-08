if [ -z $2 ]
then
    echo "usage: getindstat connection outfile [password]"
    exit 1
fi

if [ -z $3 ]
then
    authCmd=""
else
    authCmd="db.auth('$3','$4');"
fi
echo $authCmd
    
mongo $1 <<EOF >$2
use admin;
$authCmd
rs.slaveOk();
load("allCollStats.js");
let skip = ["admin","config","test", "local"];
// Switch to admin database and get list of databases.
db = db.getSiblingDB("admin");
let dbs = db.runCommand({ "listDatabases": 1 }).databases;
print('{ _id: {Server: "$1", DB: "all", Collection: "all"}, Databaselist: ');
printjson(dbs);
print("}");
// Iterate through each database and get its collections.
dbs.forEach(function(database) {
    db = db.getSiblingDB(database.name);
    db.getName();
    if (skip.indexOf(db.getName()) == -1)
    allCollStats("$1");

});
exit
EOF

ed $2 <<EOF >/dev/null
1,6d
p
$
d
p
wq
EOF
echo Import $2 with mongoimport
