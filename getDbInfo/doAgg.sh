echo $1
mongo "$1" <<EOF
load(`dirname $0`/doagg.js)
aggStats($2)
EOF

echo "Server,DB,NS,TotalSize,Count" > $2_indexsizes.csv
mongoexport --uri "$1" -c $2_ind_ts --type csv -f "_id.Server,_id.DB,_id.Collection,TotalSize,Count" --noHeaderLine >> $2_indexsizes.csv
echo "Server,Idx Path,Index,Size,Ops,DB,Collection,Unused,IsId,Days" > $2_merged.csv
mongoexport --uri "$1" -c $2_merged --type csv -f "ns.Server,ns.name,index,size,Ops.0.Ops,nsid.DB,nsid.Collection,Ops.0.Unused,Ops.0.IsId,Ops.0.Days,indVer" --noHeaderLine >> $2_merged.csv
