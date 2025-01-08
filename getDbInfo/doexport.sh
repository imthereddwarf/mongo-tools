echo $1

echo "Server,DB,NS,TotalSize,Count" > $3_indexsizes.csv
mongoexport --uri "$1" -c $3_ind_ts --type csv -f "_id.Server,_id.DB,_id.Collection,TotalSize,Count" --noHeaderLine >> $3_indexsizes.csv
echo "Server,Idx Path,Index,Size,Ops,DB,Collection,Unused,IsId,Days" > $3_merged.csv
mongoexport --uri "$1" -c $3_merged --type csv -f "ns.Server,ns.name,index,size,Ops.0.Ops,nsid.DB,nsid.Collection,Ops.0.Unused,Ops.0.IsId,Ops.0.Days,indVer" --noHeaderLine >> $3_merged.csv
