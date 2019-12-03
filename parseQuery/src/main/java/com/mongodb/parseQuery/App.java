package com.mongodb.parseQuery;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Filters.eq;

import java.io.FileInputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;

import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.MinKey;
import org.bson.types.MaxKey;
import org.bson.types.ObjectId;

import com.mongodb.BulkWriteResult;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;

/**
 * Hello world!
 *
 */
public class App 
{

	static boolean hasNIN=false;
	static boolean hasNE = false;
	
	public static void main( String[] args ) throws InterruptedException
    {
		int i;
		Pattern dotKey = Pattern.compile("\\{ *([^.:{}]*\\.[^:]*):");
        System.out.println(dotKey.pattern());		
		System.out.println("Java: "+System.getProperty("java.vendor")+ " " + System.getProperty("java.version") + " OS: "+System.getProperty("os.name")+" " +System.getProperty("os.version"));
		String propsFile = "default.properties";
		
		if (args.length == 2 && args[0].equalsIgnoreCase("--config")) {
			propsFile = args[1];
		}
		// create and load default properties
		Properties defaultProps = new Properties();
		try {
			FileInputStream in = new FileInputStream(propsFile);
			defaultProps.load(in);
			in.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
			return;
		}

	
		System.out.println("Connection is: "+defaultProps.getProperty("SourceConnection"));
		MongoClientURI srcURI = new MongoClientURI(defaultProps.getProperty("SourceConnection"));
		
		MongoClient sourceClient = new MongoClient(srcURI);
        MongoDatabase sourceDB = sourceClient.getDatabase(defaultProps.getProperty("SourceDatabase"));
        

        
        MongoCollection<Document> srcColl = sourceDB.getCollection(defaultProps.getProperty("SourceCollection"));

        MongoCursor<Document> srcCur = srcColl.find(new Document("Command", new Document("$exists",true))).iterator();
 
        List<WriteModel<Document>> writes = new ArrayList<WriteModel<Document>>();
        int batchCount = 0;
        int numBatches = 0;
        
        while (srcCur.hasNext()) {
        	Document thisDoc = srcCur.next();
    		String cmd = thisDoc.getString("Command");
    		int split = cmd.indexOf("{");
    		String cmdName = cmd.substring(0, split-1);
    		Document cmdDoc = null;
    		try {
    			Matcher matcher = dotKey.matcher(cmd.substring(split));
    			if (matcher.find()) {
    				if (matcher.groupCount() == 1)
    					cmd = cmd.replace(matcher.group(1), "\""+matcher.group(1)+"\"");
    				else
    					System.out.println(matcher.groupCount());
    			}
    			cmdDoc = Document.parse(cmd.substring(split));
    		} catch (Exception e) {
    			System.out.println(e.getMessage());
    			System.out.println(cmd.substring(split));
    			continue;
    		}
    		Document filter = null;
    		Document sort=null;
    		Document hint=null;
    		Object val = null;
    		Integer limit=null;
    		hasNIN=false;
    		hasNE=false;
    		
    		if (cmdName.equals("find")) {
    				filter = cmdDoc.get("filter",Document.class);
    				sort = cmdDoc.get("sort",Document.class);
    				hint = cmdDoc.get("hint",Document.class);
    				val = cmdDoc.get("limit");
    				limit = (Integer)val;
    		}
    		else if (cmdName.equals("count")||cmdName.contentEquals("findAndModify")) {
				filter = cmdDoc.get("query",Document.class);
    		}
    		else if (cmdName.equals("getMore")) continue;
    		else if (cmdName.equals("insert")) continue;
    		else if (cmdName.contentEquals("aggregate")) {
    			ArrayList<Document> pipeline = (ArrayList<Document>)cmdDoc.get("pipeline");
    			for (i=0;i<pipeline.size();i++) {
    				Document stage = pipeline.get(i);
    				String stageName = stage.keySet().iterator().next();
    				if (stageName.contentEquals("$match"))
    						filter = stage.get("$match",Document.class);		
    			}
    		}
    		else {
    			System.out.println(cmdName);
    		}
    		Document changes = new Document("cmdType",cmdName);
    		if (filter != null) changes.append("filter", fmtQuery(filter));
    		if (sort != null) changes.append("sort", fmtQuery(sort));
    		if (hint != null) changes.append("hint", fmtQuery(hint));
    		if (limit != null) changes.append("limit", limit);
    		changes.append("hasNE", hasNE);
    		changes.append("hasNIN", hasNIN);
    		
    		Document updDoc = new Document("_id",thisDoc.getObjectId("_id"));
    		writes.add(
    				new UpdateOneModel<Document>(updDoc,new Document("$set",changes))
    				);
    		batchCount++;
    		if (batchCount > 999) {
    			try {
    				com.mongodb.bulk.BulkWriteResult bulkWriteResult = srcColl.bulkWrite(writes);
    				numBatches++;
    				System.out.println("Batch: "+numBatches+" Updated: "+bulkWriteResult.getModifiedCount());
    				batchCount = 0;
    				writes.clear();
    			}
    			catch (Exception e) {
    				System.out.println(e.getMessage());
    			}
    		}
    		
    		//System.out.println(cmdName+": "+fmtQuery(filter)+" Sort: "+fmtQuery(sort)+" Hint: "+fmtQuery(hint)+" Limit: "+limit.toString());
    		
        }
		if (batchCount > 0) {
			try {
				com.mongodb.bulk.BulkWriteResult bulkWriteResult = srcColl.bulkWrite(writes);
				System.out.println("Final Batch - Updated: "+bulkWriteResult.getModifiedCount());
				batchCount = 0;
				writes.clear();
			}
			catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
        		
    }
	static private String fmtQuery(Document filter) {
		Set<String> keys = filter.keySet();
		Iterator<String> iterator = keys.iterator();
		String queryShape = "{";
		while(iterator.hasNext()){
		  if (!queryShape.equals("{")) queryShape = queryShape.concat(",");
		  String key = iterator.next();
		  Object value = filter.get(key);
		  String valtxt = null;
		  if (value instanceof Document) {
			  Set<String> subkeys = ((Document)value).keySet();
			  Iterator<String> subit = subkeys.iterator();
			  String subkey = subit.next();
			  if (subkey.toLowerCase().contentEquals("$nin")) hasNIN=true;
			  if (subkey.toLowerCase().contentEquals("$ne")) hasNE=true;
			  Object subval = ((Document) value).get(subkey);
			  if (subval instanceof ArrayList) {
				  valtxt = key+ ": {"+subkey+": "+"["+String.valueOf(((ArrayList)subval).size())+"]}";
			  } else if (subval instanceof Document) {
				  valtxt = key+ ": {"+subkey+": {}}";
			  } else {
				  valtxt = key+ ": {"+subkey+": 1}";
			  }
				  
		  } else {
			  valtxt = key+": 1";
		  }
		  queryShape = queryShape.concat(valtxt);
		}
		queryShape = queryShape.concat("}");
		return queryShape;
	}
}
