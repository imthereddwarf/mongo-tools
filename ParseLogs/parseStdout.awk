BEGIN {
    got["find"] = 1;
    got["getMore"] = 1;
    got["update"] = 1;
    got["aggregate"] = 1;
    got["remove"] = 1;
    got["findAndModify"] = 1;
    got["insert"] = 1;
    got["count"] = 1;
    nlines = 8
    nfound = 0;
}

NR == 1 {
    n=split (FILENAME,a,/\//);
    outFile = a[n] ".json";
    print outFile;
}

/^2019.* NETWORK / {
    split($9,ip,":");
    key = ip[1] "-" $15 $17;
    Drivers[key]++;
}

/^2019.* COMMAND/ {
    # Merge any split lines back together
    if (!match($NF,/^[0-9]+ms$/)) {
	longline = $0;
	getline;
	while (!match($NF,/^[0-9]+ms$/)) {
	    longline = longline "\\n" $0;
	    getline;
	}
	longline = longline " " $0;
	count = split(longline,a,FS);
	for (i=1;i<=count;i++) $i = a[i];
    }
# split the line into components
    operation=$8
    collection=$6
    if (operation != "aggregate" && operation != "getMore" &&
        operation != "findAndModify" && operation != "insert" &&
        operation != "find" && operation != "count") next;    
#    if (operation == "delete" || operation == "update") next;   #Get delete and update from WRITE
#    if (got[$8]) {
#	got[$8] = 0;
#	nfound++;
#	if (nfound >= nlines) nextfile;
#    }
#    else next;
    cpos = ipos = lpos = ppos = kpos = qpos = upos = prpos = npos = curpos = 0;
    for (i=7;i<NF;i++) {
	if (substr($i,1,10) == "ninserted:") ipos = i;
	if ($i == "planSummary:") ppos = i;
	if ($i == "command:") cpos = i;
	if ($i == "query:") qpos = i;
	if ($i == "update:") upos = i;
	if ($i == "new:") npos = i;
	if (substr($i,1,9) == "protocol:") prpos = i;
	if (substr($i,1,9) == "cursorid:") curpos = i;	
	if (substr($i,1,13) == "keysExamined:") kpos = i;
	if (substr($i,1,6) == "locks:") lpos = i;
    }
    curStr = "";
    if (operation == "insert") {
	if (cpos && ipos && (ipos > cpos)) {
	    cmdStr = $(cpos+1)
	    for (i=cpos+2;i<ipos;i++) cmdStr = cmdStr " " $i;
	    gsub(/'/,"\"",cmdStr);
	    gsub(/\\/,"\\\\",cmdStr);	    
	}
	if ( ipos && lpos && (lpos > ipos)) {
	    statStr = $(ipos)
	    for (i=ipos+1;i<lpos;i++) statStr = statStr ", " $i;
	}
    }
    else if (operation == "findAndModify") {
	if (cpos && ppos && (ppos > cpos)) {
	    cmdStr = $(cpos+1)
	    for (i=cpos+2;i<ppos;i++) cmdStr = cmdStr " " $i;
	    gsub(/'/,"\"",cmdStr);
	    gsub(/\\/,"\\\\",cmdStr);	    
	}
	if (ppos && upos && (upos > ppos)) {
	    planStr = $(ppos+1)
	    for (i=ppos+2;i<upos;i++) planStr = planStr " "$i;
	}
 	if (upos && kpos && (kpos > upos)) {
	    updStr = $(upos+1)
	    for (i=upos+2;i<kpos;i++) updStr = updStr " "$i;
	    gsub(/'/,"\"",updStr);
	    gsub(/\\/,"\\\\",updStr);	   	    	    
	}
	if (kpos && lpos && (lpos > kpos)) {
	    statStr = $(kpos)
	    for (i=kpos+1;i<lpos;i++) statStr = statStr "," $i;
	}
    }
    else {
	if (cpos && ppos && (ppos > cpos)) {
	    cmdStr = $(cpos+1)
	    for (i=cpos+2;i<ppos;i++) cmdStr = cmdStr " " $i;
	    gsub(/'/,"\"",cmdStr);
	    gsub(/\\/,"\\\\",cmdStr);	    	    
	}
	if (curpos > 0) {
	    if (ppos && (curpos > ppos)) {
		planStr = $(ppos+1)
		for (i=ppos+2;i<curpos;i++) planStr = planStr " "$i;
	    }
	    curStr = $curpos;
	}
	else {
	    if (ppos && kpos && (kpos > ppos)) {
		planStr = $(ppos+1)
		for (i=ppos+2;i<kpos;i++) planStr = planStr " "$i;
	    }
	    curStr = "";
	}
 
	if (kpos && lpos && (lpos > kpos)) {
	    statStr = $(kpos)
	    for (i=kpos+1;i<lpos;i++) statStr = statStr "," $i;
	}
    }
    if (lpos && prpos && (prpos > lpos)) {
	lockStr = $(lpos);
	for (i=lpos+1;i<prpos;i++) lockStr = lockStr " " $i;
    }
    if (prpos) {
	protoStr = $(prpos);
	for (i=prpos+1;i<NF;i++) protoStr = protoStr " " $i;
	i = sub(/:/,":'",protoStr)
	protoStr = protoStr "'";
    }
    
    time = $NF;
    sub(/ms/,"",time);
 
    print "{" > outFile;
    print "   File: '" FILENAME "'," > outFile;
    print "   ts: ISODate(\"" $1 "\")," > outFile;
    print "   conn: '" $4 "'," >outFile;
    print "   Operation: \"" operation "\"," > outFile;
    print "   Collection: \"" collection "\"," > outFile;
    print "   Command: '" cmdStr "'," > outFile;
    if (operation != "insert")
	print "   Plan: '" planStr "'," > outFile;
    if (match(curStr,/.*:[0-9]+/))
	print "   " curStr "," > outFile;
    if (operation == "findAndModify")
	print "   Update: '" updStr "'," > outFile;
    print "   Stats: {" statStr "}," > outFile;
    print "   " lockStr "," > outFile;
    print "   " protoStr "," > outFile;
    print "   Time: " time > outFile;
    print "}" > outFile;
    
}

/^2019.* WRITE/ {
# Merge any split lines back together
    if (!match($NF,/^[0-9]+ms$/)) {
	longline = $0;
	getline;
	while (!match($NF,/^[0-9]+ms$/)) {
	    longline = longline "\\n" $0;
	    getline;
	}
	longline = longline " " $0;
	count = split(longline,a,FS);
	for (i=1;i<=count;i++) $i = a[i];
    }
    operation=$5
    collection=$6
    if (operation != "update" && operation != "remove") next;
#    if (got[$5]) {
#	got[$5] = 0;
#	nfound++;
#	if (nfound >= nlines) nextfile;
#    }
#    else next;
    qpos = lpos = ppos = kpos = upos = 0;
    for (i=7;i<NF;i++) {
	if ($i == "query:") qpos = i;
	if ($i == "planSummary:") ppos = i;
	if (substr($i,1,7) == "update:" && $(i+1) == "{") upos = i;
	if (substr($i,1,13) == "keysExamined:") kpos = i;
	if (substr($i,1,6) == "locks:") lpos = i;
    }
    if (qpos && ppos && (ppos > qpos)) {
	queryStr = $(qpos+1)
	for (i=qpos+2;i<ppos;i++) queryStr = queryStr " " $i;
	gsub(/'/,"\"",queryStr);
	gsub(/\\/,"\\\\",queryStr);	
    }
    if (operation == "update") {
	if ( ppos && upos && (upos > ppos)) {
	    planStr = $(ppos+1)
	    for (i=ppos+2;i<upos;i++) planStr = planStr " " $i;
	}
	if ( upos && kpos && (kpos > upos)) {
	    updStr = $(upos+1)
	    for (i=upos+2;i<kpos;i++) updStr = updStr " " $i;
	}
	gsub(/'/,"\"",updStr);
	gsub(/\\/,"\\\\",updStr);		
    }
    else if (operation == "remove") {
	if (ppos && kpos && (kpos > ppos)) {
	    planStr = $(ppos+1)
	    for (i=ppos+2;i<kpos;i++) planStr = planStr " "$i;
	}
    }
    if (kpos && lpos && (lpos > kpos)) {
	statStr = $(kpos)
	for (i=kpos+1;i<lpos;i++) statStr = statStr "," $i;
    }
    if (lpos) {
	lockStr = $(lpos)
	for (i=lpos+1;i<NF;i++) lockStr = lockStr " " $i;
    }
	
    time = $NF
    sub(/ms/,"",time);
    print "{" > outFile;
    print "   File: '" FILENAME "'," > outFile;
    print "   ts: ISODate(\"" $1 "\")," > outFile;
    print "   conn: '" $4 "'," >outFile;    
    print "   Operation: \"" operation "\"," > outFile;
    print "   Collection: \"" collection "\"," > outFile;
    print "   Query: '" queryStr "'," > outFile;
    print "   Plan: \"" planStr "\"," > outFile;
    if (operation == "update")    
	print "   Update: '" updStr "'," > outFile;
    print "   Stats: {" statStr "}," > outFile;
    print "   " lockStr "," > outFile;
    print "   Time: " time > outFile;
    print "}" > outFile;
}

/^2019.* REPL/ {
# Merge any split lines back together
    if (!match($NF,/^[0-9]+ms$/)) {
	longline = $0;
	getline;
	while (!match($NF,/^[0-9]+ms$/)) {
	    longline = longline "\\n" $0;
	    getline;
	}
	longline = longline " " $0;
	count = split(longline,a,FS);
	for (i=1;i<=count;i++) $i = a[i];
    }
    operation=$10
#    collection=$6
#    if (operation != "update" && operation != "remove") next;
#    if (got[$5]) {
#	got[$5] = 0;
#	nfound++;
#	if (nfound >= nlines) nextfile;
#    }
#    else next;
    tpos = oppos = npos = wpos = opos = o2pos = 0;
    longOp = 0;
    start = 11;
    if ($8 != "applied" || $9 != "op:") {
	longline = $12;
	longsize = substr(longline,2,length(longline)-4)*1024;
	longOp = 1;
	print longline, longsize;
	for(i=9;i<NF;i++) {
	    if ($i == "applied" && $(i+1) == "op:") {
		start = i+3;
		break;
	    }
	}
    }
    for (i=start;i<NF;i++) {
	if ($i == "ts:" && tpos == 0) tpos = i;
	if ($i == "op:" && oppos == 0) oppos = i;
	if ($i == "ns:" && npos == 0) npos = i;
	if ($i == "o2:" && o2pos == 0) o2pos = i;
	if ($i == "wall:" && wpos == 0 ) wpos = i;
	if ($i == "o:" && opos == 0) opos = i;
    }
    if (tpos) {
	if ($(tpos+1) == "[") {
	    tsStr = "[ ";
	    i = tpos+2;
	    while ($i != "],") {
		tsStr = tsStr $i " ";
		i++
	    }
	    tsStr = tsStr  "],";
	}
	else
	    tsStr = $(tpos+1) " " $(tpos+2);
    }
    if (oppos) 
	opStr = $(oppos+1)
    if (npos)
	nsStr = $(npos+1)
    if (wpos)
	wallStr = $(wpos+1) " " $(wpos+2)

    if (o2pos && wpos && (wpos > o2pos)) {
	o2Str = $(kpos)
	for (i=o2pos+1;i<wpos;i++) o2Str = o2Str "," $i;
    }
    if (opStr == "\"u\"," || opStr == "\"i\",") {
	if ( opos ) {
	    updStr = $(opos+1)
	    for (i=opos+2;i<(NF-2);i++) updStr = updStr " " $i;
	}
	gsub(/'/,"\"",updStr);
	gsub(/\\/,"\\\\",updStr);		
    }
	
    time = $NF
    sub(/ms/,"",time);
    print "{" > outFile;
    print "   File: '" FILENAME "'," > outFile;
    print "   ts: ISODate(\"" $1 "\")," > outFile;
    print "   Operation: \"Repl\","  > outFile;
    print "   SubOp: " opStr  > outFile;
    print "   Collection: " nsStr  > outFile;
    print "   OpTime: " tsStr > outFile;
    print "   WallTime: " wallStr  > outFile;
    if (opStr == "\"u\",") {
	if (length(updStr) > 200) 
	    print "   Update: '" substr(updStr,1,200) "'," > outFile;
	else
	    print "   Update: '" updStr "'," > outFile;
	if (longOp) {
	    print "   OpLen: " longsize ","  >outFile;
	    print "   LogLen: \"" longline "\"," >outFile;
	}
	else
	    print "   OpLen: " length(updStr) ","  >outFile;	    
    }
    if (opStr == "\"i\",") {
	if (length(updStr) > 200) 
	    print "   Insert: '" substr(updStr,1,200) "'," > outFile;
	else
	    print "   Insert: '" updStr "'," > outFile;
	if (longOp) {
	    print "   OpLen: " longsize ","  >outFile;
	    print "   LogLen: \"" longline "\"," >outFile;
	}
	else
	    print "   OpLen: " length(updStr) ","  >outFile;
    }
    print "   Time: " time > outFile;
    print "}" > outFile;
}

/action.completed_coll/ {
    time = $NF
    sub(/ms/,"",time);
    x = time + 0;
    if ($14 == "pid:") pidcount[$15]++;
    if ($14 == "action:") {
      key = $15 "=" $17
#      if (key == "{=[" &&  x > 10) print $0;
      filterCount[key]++;
      filterTime[key] += time;
    }
}

END {
    for (cmd in tcount) {
	print cmd,tcount[cmd],ttime[cmd]/tcount[cmd];
    }
#    print "Clients"
#    for (cmd in Drivers) {
#	print cmd,Drivers[cmd];
#    }
#    for (pid in pidcount) {
#	if (pidcount[pid] > 9) 
#	    print pid,pidcount[pid]
#    }
#    for (flt in filterCount) {
#	if (filterCount[flt] > 2) 
#	print flt "," filterCount[flt], ",",filterTime[flt];
#    }
}
