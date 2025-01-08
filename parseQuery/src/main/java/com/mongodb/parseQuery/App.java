package com.mongodb.parseQuery;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Filters.eq;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;

import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.MinKey;
import org.bson.types.MaxKey;
import org.bson.types.ObjectId;

import com.mongodb.BulkWriteResult;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOptions;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
/*
 * Test compass
 */
import java.util.Arrays;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Filters.type;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Projections.computed;
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

		//Pattern dotKey = Pattern.compile("\\{ *([^.:{}]*\\.[^:]*):");


		System.out.println("Java: "+System.getProperty("java.vendor")+ " " + System.getProperty("java.version") + " OS: "+System.getProperty("os.name")+" " +System.getProperty("os.version"));
		String propsFile = "default.properties";
		
		String parseField = "Command";
		
		
		if (args.length >= 2) {
			if (args[0].equalsIgnoreCase("--config")) {
				propsFile = args[1];
				if (args.length == 3) parseField = args[2];
			} else if (args[1].equalsIgnoreCase("--config")) {
				propsFile = args[2];
				parseField = args[0];
			}
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

		ConnectionString connectionString = new ConnectionString(defaultProps.getProperty("SourceConnection"));
		MongoClientSettings settings = MongoClientSettings.builder()
		        .applyConnectionString(connectionString)
		        .build();
		MongoClient mongoClient = MongoClients.create(settings);
		MongoDatabase sourceDB = mongoClient.getDatabase(defaultProps.getProperty("SourceDatabase"));

        
        SimpleDateFormat formatter = new SimpleDateFormat("MM-dd-yyyy hh:mm:ss a", Locale.ENGLISH);
        formatter.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
         
        String originalDate = defaultProps.getProperty("OriginalDate","NotSet");
        Date runTime = null; 
        if (originalDate.contentEquals("NotSet")) 
        	runTime = new Date();  /* Now */
		else
			try {
				runTime = formatter.parse(originalDate);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        
        
        MongoCollection<Document> srcColl = sourceDB.getCollection(defaultProps.getProperty("SourceCollection"));

        if (parseField.contentEquals("Command"))
        	fmtCommand(srcColl);
        else if (parseField.contentEquals("Update")) 
        	fmtUpdate(srcColl);
        else if (parseField.contentEquals("jmeter"))
        	fmtJmeter(srcColl);
        else if (parseField.contentEquals("genJmeter")) {
        	MongoCollection<Document> dataColl = sourceDB.getCollection(defaultProps.getProperty("DataCollection") );
        	genJmeter(srcColl,dataColl, runTime);
        }
        		
    }
	
	static private void fmtCommand(MongoCollection<Document> srcColl ) {
		
		int i;
		Pattern dotKey = Pattern.compile("([{,] *)([^.:{}, ]*\\.[^: },]*):");
		Pattern bindata = Pattern.compile("BinData\\([0-9], ([0-9A-F.]*)\\)");
		Pattern timestamp = Pattern.compile("Timestamp ([0-9]*)000\\|([0-9]*)([ ,}])");
		Pattern forSpireon = Pattern.compile("\\\"(\\[\\{[^\\\"}]*((\\\")[^\\\"}]*)*)\\\""); 
		Pattern imbedQuote = Pattern.compile(":\\s*\\\"(([^\\\"]|(\\\"(?!\\s*[,}])))*)\\\"(?= *[,}])");
		
		MongoCursor<Document> srcCur = srcColl.find(new Document("Command", new Document("$exists",true))).iterator();
		 
        List<WriteModel<Document>> writes = new ArrayList<WriteModel<Document>>();
        int batchCount = 0;
        int numBatches = 0;
        
        while (srcCur.hasNext()) {
        	Document thisDoc = srcCur.next();
        	Document Stats = thisDoc.get("Stats",Document.class);
        	Integer docs = 0;
        	Integer returned = 0;
        	if (Stats != null) {
        		docs = Stats.getInteger("docsExamined");
        	    returned = Stats.getInteger("nreturned");
        	}
        	double ratio = 0.0;
        	if (docs != 0 ) {
        		if (returned > 0 ) 
        			ratio = docs/returned;
        		else 
        			ratio = docs/1;
        	}
        	
    		String cmd = thisDoc.getString("Command");
    		int split = cmd.indexOf("{");
    		String cmdName = cmd.substring(0, split-1);
    		if (cmdName.contentEquals("getMore")) {
        		cmd = thisDoc.getString("originatingCommand");
        		if (cmd == null || cmd.contentEquals("")) continue;
        		split=0;
    		}

    		Document cmdDoc = null;
    		try {
    			Matcher matcher = dotKey.matcher(cmd);
    			if (matcher.find()) {
    				if (matcher.groupCount() == 2) {
    					//System.out.println(cmd);
    					cmd = matcher.replaceAll("$1\"$2\":");
    					//System.out.println(cmd);
    				}
    					// bcmd = cmd.replace(matcher.group(1), "\""+matcher.group(1)+"\"");
    				else
    					System.out.println(matcher.groupCount());
    			}

    			matcher = bindata.matcher(cmd);
    			if (matcher.find()) {
    				if (matcher.groupCount() == 1)
    					cmd = matcher.replaceAll("\"$1\"");
    				else
    					System.out.println(matcher.groupCount());
    			}
    			matcher = timestamp.matcher(cmd);
    			if (matcher.find()) {
    				if (matcher.groupCount() == 3)
    					cmd = matcher.replaceAll("Timestamp($1,$2)$3");
    				else
    					System.out.println(matcher.groupCount());
    			}
    			matcher = forSpireon.matcher(cmd);
    			if (matcher.find()) {
    				if (matcher.groupCount() == 3)
    					cmd = matcher.replaceAll("'$1'");
    				else
    					System.out.println(matcher.groupCount());
    			} 
    			cmdDoc = Document.parse(cmd.substring(split));
    		} catch (Exception e) {
    			System.out.println(thisDoc.getObjectId("_id").toString());
    			System.out.println(e.getMessage());
    			System.out.println(cmd.substring(split));
    			continue;
    		}
    		String opname = cmdDoc.getString("Operation");
    		Document filter = null;
    		Document writeConcern = null;
    		Document updFields = null;
    		Document sort=null;
    		Document hint=null;
    		Object val = null;
    		Integer limit=null;
    		hasNIN=false;
    		hasNE=false;
    		
    		try {
    			writeConcern = cmdDoc.get("writeConcern",Document.class);
    		}
    		catch (Exception e) {
    			writeConcern = null;
    		}
			if (writeConcern == null  && 
					(cmdName.contentEquals("insert") || cmdName.contentEquals("update") ||
					cmdName.contentEquals("remove") || cmdName.contentEquals("findAndModify"))) {
				writeConcern = new Document("w","default");
			}
 
    		
    		if (cmdName.equals("find")) {
    				filter = cmdDoc.get("filter",Document.class);
    				sort = cmdDoc.get("sort",Document.class);
    				hint = cmdDoc.get("hint",Document.class);
    				val = cmdDoc.get("limit");
    				try {
    					limit = (Integer)val;
    				}
    				catch (ClassCastException e) {
    					limit = 1;
    				}
    		}
    		else if (cmdName.equals("count")||cmdName.contentEquals("findAndModify")) {
				filter = cmdDoc.get("query",Document.class);
    		}
    		else if (cmdName.equals("getMore")) continue;
    		else if (cmdName.equals("insert")) {
    			opname = "insert";
    		}
    		else if (cmdName.contentEquals("aggregate")) {
    			ArrayList<Document> pipeline = (ArrayList<Document>)cmdDoc.get("pipeline");
    			for (i=0;i<pipeline.size();i++) {
    				Document stage = pipeline.get(i);
    				String stageName = stage.keySet().iterator().next();
    				if (stageName.contentEquals("$match")) {
    						filter = stage.get("$match",Document.class);	
    						break;
    				}
    			}
    		}
    		else if (cmdName.contentEquals("distinct")) {
    			filter = cmdDoc.get("query",Document.class);
    		}
    		else if (opname == null) continue;
    		else if (opname.equals("Repl")) continue;
    		else if (opname.equals("update")) {
				updFields = cmdDoc.get("filter.$set",Document.class);	
    		}
    		else if (opname.equals("remove")) {
				updFields = cmdDoc.get("filter.$set",Document.class);	
    		}
    		else if (opname.equals("insert")) {
				updFields = cmdDoc.get("filter.$set",Document.class);	
    		}
    		else {
    			System.out.println(cmdName);
    		}
    		Document changes = new Document("cmdType",cmdName);
    		if (filter != null) {
        		ArrayList params = new ArrayList();
    			changes.append("filter", fmtQuery(filter,params));
    			changes.append("filter_params", params);
    		}
    		if (ratio != 0.0) changes.append("Ratio", ratio);
    		if (sort != null) changes.append("sort", fmtQuery(sort));
    		if (hint != null) changes.append("hint", fmtQuery(hint));
    		if (limit != null) changes.append("limit", limit);
    		if (writeConcern != null) {
    			changes.append("writeConcern", writeConcern);
    		}
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
    				System.out.println(writes.get(0).toString());
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
	
	static private void fmtUpdate(MongoCollection<Document> srcColl ) {
		
		
		int i;
		Pattern dotKey = Pattern.compile("([^.:{}, ]*\\.[^: },]*):");
		Pattern imbedquote = Pattern.compile(": *(\"[^\"]*\"[^\"]*\")([,}])");
		Pattern smallNumber = Pattern.compile(": *([+-]?[0-9]+e-?[0-9]+.[0-9]+)");
		Pattern operator = Pattern.compile("(\\$[a-zA-Z]*):");
		System.out.println(imbedquote.pattern());
		Pattern bindata = Pattern.compile("BinData\\([0-9], ([0-9A-F.]*)\\)");
		
		MongoCursor<Document> srcCur = srcColl.find(new Document("Update", new Document("$exists",true))).iterator();
		 
        List<WriteModel<Document>> writes = new ArrayList<WriteModel<Document>>();
        int batchCount = 0;
        int numBatches = 0;
        
        while (srcCur.hasNext()) {
        	Document thisDoc = srcCur.next();
        	//System.out.println(thisDoc.toString());
    		String cmd = thisDoc.getString("Update");
    		if (cmd.contains("answer")) {
    			System.out.println(cmd);
    		}
    		//System.out.println(cmd);

    		Document cmdDoc = null;
    		try {
    			Matcher matcher = dotKey.matcher(cmd);
    			if (matcher.find()) {
    				if (matcher.groupCount() == 1) {
    					//System.out.println(cmd);
    					cmd = matcher.replaceAll("\"$1\":");
    					//System.out.println(cmd);
    				}
    					// bcmd = cmd.replace(matcher.group(1), "\""+matcher.group(1)+"\"");
    				else
    					System.out.println(matcher.groupCount());
    			}

    			matcher = imbedquote.matcher(cmd);
    			if (matcher.find()) {
    				if (matcher.groupCount() == 2) {
    					//System.out.println(cmd);
    					cmd = matcher.replaceAll(": '$1'$2");
    					//System.out.println(cmd);
    				}
    					// bcmd = cmd.replace(matcher.group(1), "\""+matcher.group(1)+"\"");
    				else
    					System.out.println(matcher.groupCount());
    			}
    			
    			matcher = smallNumber.matcher(cmd);
    			if (matcher.find()) {
    				if (matcher.groupCount() == 1) {
    					//System.out.println(cmd);
    					cmd = matcher.replaceAll(": 0.01");
    					//System.out.println(cmd);
    				}
    					// bcmd = cmd.replace(matcher.group(1), "\""+matcher.group(1)+"\"");
    				else
    					System.out.println(matcher.groupCount());
    			}

    			matcher = bindata.matcher(cmd);
    			if (matcher.find()) {
    				if (matcher.groupCount() == 1) {
    					cmd = matcher.replaceAll("\"$1\""); 
    					//System.out.println(cmd);
    				}
    				else
    					System.out.println(matcher.groupCount());
    			}
    			/* matcher = operator.matcher(cmd);
    			if (matcher.find()) {
    				if (matcher.groupCount() == 1) {
    					cmd = matcher.replaceAll("\"$1\":"); 
    					//System.out.println(cmd);
    				}
    				else
    					System.out.println(matcher.groupCount());
    			}
    			*/
    			cmdDoc = Document.parse(cmd); 
    		} catch (Exception e) {
    			System.out.println(thisDoc.getObjectId("_id").toString());
    			System.out.println(e.getMessage());
    			System.out.println(cmd);
    			continue;
    		}
    		Set<String> keys = cmdDoc.keySet();
    		if (keys.size() < 3 || keys.size() > 8 ) {
    			//System.out.println(thisDoc.getObjectId("_id").toString());
    			//System.out.println(String.valueOf(keys.size()));
    		}
    		Boolean upsert = cmdDoc.getBoolean("upsert");
    		Boolean multi = cmdDoc.getBoolean("multi");
    		/*
    		Document u = (Document)cmdDoc.get("Update");
    		if (u == null) {
    			System.out.println(thisDoc.toString());
    		}
    		*/
    		Document filter = null;
    		String fldQuery = thisDoc.getString("Query");  
    		if (fldQuery != null) {
    			Matcher matcher = bindata.matcher(fldQuery);
    			if (matcher.find()) {
    				if (matcher.groupCount() == 1)
    					fldQuery = matcher.replaceAll("\"$1\"");
    				else
    					System.out.println(matcher.groupCount());
    			}
    			try {
    				filter = Document.parse(fldQuery);
    			} catch (Exception e) {
    				System.out.println(e.getMessage());
    				System.out.println(fldQuery);
    				filter = null;
    			}
    		}
    		//Document updFields = (Document)u.get("$set");
    		Document soi = (Document)cmdDoc.get("$setOnInsert");
    		Document set=  (Document)cmdDoc.get("$set");
    		Document hint=null;
    		Object val = null;
    		Integer limit=null;
    		hasNIN=false;
    		hasNE=false;
    		
 
    		Document changes = new Document("upsert",upsert);
    		if (filter != null) {
        		ArrayList params = new ArrayList();
    			changes.append("filter", fmtQuery(filter,params));
    			changes.append("filter_params", params);
    		}
    		if (soi != null) {
        		ArrayList params = new ArrayList();
    			changes.append("soi", fmtQuery(soi,params));
    			changes.append("soi_params", params);
    		}
    		if (set != null) {
        		ArrayList params = new ArrayList();
    			changes.append("set", fmtQuery(set,params));
    			changes.append("set_params", params);
    		}
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
	


static private void fmtJmeter(MongoCollection<Document> srcColl ) {
		

		MongoCursor<Document> srcCur = srcColl.find(new Document("query", new Document("$exists",true))).iterator();
		 
        List<WriteModel<Document>> writes = new ArrayList<WriteModel<Document>>();
        int batchCount = 0;
        int numBatches = 0;
        Document filter = null;
        
        while (srcCur.hasNext()) {
        	Document thisDoc = srcCur.next();
        	Integer limit = 0;
        	try {
        		limit = thisDoc.getInteger("limit");
        	} catch (Exception e) {
        		limit = 0;
        	}
        	String dbName = thisDoc.getString("db");
        	String collName = thisDoc.getString("collection");
        	String query = thisDoc.getString("query");
        	String projection = thisDoc.getString("projection");
        	String sort = thisDoc.getString("sort");
        	String dc = thisDoc.getString("doCount");
        	boolean doCount = dc.contentEquals("TRUE");

        	try {
        		filter = Document.parse(query);
        	} 
        	catch (Exception e) {
        		filter = null; 
        	}
        	
    		Document changes = new Document("ns",dbName.concat(".").concat(collName)).append("hasCount",doCount);
    		
    		if (filter != null) {
        		ArrayList params = new ArrayList();
    			changes.append("filter", fmtQuery(filter,params));
    			changes.append("filter_params", params);
    		}

    		if (limit == 0) changes.append("limit", limit); 
    		
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
    				System.out.println(writes.get(0).toString());
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


static private void genJmeter(MongoCollection<Document> srcColl, MongoCollection<Document> dataColl, Date runTime ) {
	

	
	MongoCursor<Document> srcCur = srcColl.find(new Document("Stage Rank", new Document("$exists",true))).sort(new Document("Prd Rank",1)).noCursorTimeout(true).iterator();
	 

    Integer threadCount = 0;

    String jmxHeader = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" + 
    		"<jmeterTestPlan version=\"1.2\" properties=\"5.0\" jmeter=\"5.1.1 r1855137\">\n" + 
    		"  <hashTree>\n" + 
    		"    <TestPlan guiclass=\"TestPlanGui\" testclass=\"TestPlan\" testname=\"Test Plan\" enabled=\"true\">\n" + 
    		"      <stringProp name=\"TestPlan.comments\"></stringProp>\n" + 
    		"      <boolProp name=\"TestPlan.functional_mode\">false</boolProp>\n" + 
    		"      <boolProp name=\"TestPlan.tearDown_on_shutdown\">true</boolProp>\n" + 
    		"      <boolProp name=\"TestPlan.serialize_threadgroups\">false</boolProp>\n" + 
    		"      <elementProp name=\"TestPlan.user_defined_variables\" elementType=\"Arguments\" guiclass=\"ArgumentsPanel\" testclass=\"Arguments\" testname=\"User Defined Variables\" enabled=\"true\">\n" + 
    		"        <collectionProp name=\"Arguments.arguments\"/>\n" + 
    		"      </elementProp>\n" + 
    		"      <stringProp name=\"TestPlan.user_define_classpath\"></stringProp>\n" + 
    		"    </TestPlan>\n" +
    		"    <hashTree>\n";
    
    String jmxThreadHeader = "            <ThreadGroup guiclass=\"ThreadGroupGui\" testclass=\"ThreadGroup\" testname=\"$$TESTNAME$$\" enabled=\"true\">\n" + 
    		"        <stringProp name=\"ThreadGroup.on_sample_error\">continue</stringProp>\n" + 
    		"        <elementProp name=\"ThreadGroup.main_controller\" elementType=\"LoopController\" guiclass=\"LoopControlPanel\" testclass=\"LoopController\" testname=\"Loop Controller\" enabled=\"true\">\n" + 
    		"          <boolProp name=\"LoopController.continue_forever\">false</boolProp>\n" + 
    		"          <stringProp name=\"LoopController.loops\">1</stringProp>\n" + 
    		"        </elementProp>\n" + 
    		"        <stringProp name=\"ThreadGroup.num_threads\">1</stringProp>\n" + 
    		"        <stringProp name=\"ThreadGroup.ramp_time\">1</stringProp>\n" + 
    		"        <boolProp name=\"ThreadGroup.scheduler\">false</boolProp>\n" + 
    		"        <stringProp name=\"ThreadGroup.duration\"></stringProp>\n" + 
    		"        <stringProp name=\"ThreadGroup.delay\"></stringProp>\n" + 
    		"      </ThreadGroup>\n" +
    		"      <hashTree>\n" + 
    		"        <LoopController guiclass=\"LoopControlPanel\" testclass=\"LoopController\" testname=\"$$LOOPNAME$$\" enabled=\"true\">\n" + 
    		"          <boolProp name=\"LoopController.continue_forever\">true</boolProp>\n" + 
    		"          <stringProp name=\"LoopController.loops\">1</stringProp>\n" + 
    		"        </LoopController>\n" + 
    		"        <hashTree>\n" + 
    		"          <JavaSampler guiclass=\"JavaTestSamplerGui\" testclass=\"JavaSampler\" testname=\"$$JAVANAME$$ \" enabled=\"true\">\n" + 
    		"            <elementProp name=\"arguments\" elementType=\"Arguments\" guiclass=\"ArgumentsPanel\" testclass=\"Arguments\" enabled=\"true\">\n" + 
    		"              <collectionProp name=\"Arguments.arguments\">\n" + 
    		"                <elementProp name=\"host\" elementType=\"Argument\">\n" + 
    		"                  <stringProp name=\"Argument.name\">host</stringProp>\n" + 
    		"                  <stringProp name=\"Argument.value\">${__P(Hostname)}</stringProp>\n" + 
    		"                  <stringProp name=\"Argument.metadata\">=</stringProp>\n" + 
    		"                </elementProp>\n" + 
    		"                <elementProp name=\"port\" elementType=\"Argument\">\n" + 
    		"                  <stringProp name=\"Argument.name\">port</stringProp>\n" + 
    		"                  <stringProp name=\"Argument.value\">27017</stringProp>\n" + 
    		"                  <stringProp name=\"Argument.metadata\">=</stringProp>\n" + 
    		"                </elementProp>\n" + 
    		"                <elementProp name=\"user\" elementType=\"Argument\">\n" + 
    		"                  <stringProp name=\"Argument.name\">user</stringProp>\n" + 
    		"                  <stringProp name=\"Argument.value\">${__P(User)}</stringProp>\n" + 
    		"                  <stringProp name=\"Argument.metadata\">=</stringProp>\n" + 
    		"                </elementProp>\n" + 
    		"                <elementProp name=\"password\" elementType=\"Argument\">\n" + 
    		"                  <stringProp name=\"Argument.name\">password</stringProp>\n" + 
    		"                  <stringProp name=\"Argument.value\">${__P(Password)}</stringProp>\n" + 
    		"                  <stringProp name=\"Argument.metadata\">=</stringProp>\n" + 
    		"                </elementProp>\n" + 
    		"                <elementProp name=\"authDB\" elementType=\"Argument\">\n" + 
    		"                  <stringProp name=\"Argument.name\">authDB</stringProp>\n" + 
    		"                  <stringProp name=\"Argument.value\">admin</stringProp>\n" + 
    		"                  <stringProp name=\"Argument.metadata\">=</stringProp>\n" + 
    		"                </elementProp>\n";
    
    String jmxData = "                <elementProp name=\"DB\" elementType=\"Argument\">\n" + 
    		"                  <stringProp name=\"Argument.name\">DB</stringProp>\n" + 
    		"                  <stringProp name=\"Argument.value\">$$DB$$</stringProp>\n" + 
    		"                  <stringProp name=\"Argument.metadata\">=</stringProp>\n" + 
    		"                </elementProp>\n" + 
    		"                <elementProp name=\"collection\" elementType=\"Argument\">\n" + 
    		"                  <stringProp name=\"Argument.name\">collection</stringProp>\n" + 
    		"                  <stringProp name=\"Argument.value\">$$COLLECTION$$</stringProp>\n" + 
    		"                  <stringProp name=\"Argument.metadata\">=</stringProp>\n" + 
    		"                </elementProp>\n" + 
    		"                <elementProp name=\"query\" elementType=\"Argument\">\n" + 
    		"                  <stringProp name=\"Argument.name\">query</stringProp>\n" + 
    		"                  <stringProp name=\"Argument.value\">$$FILTER$$</stringProp>\n" + 
    		"                  <stringProp name=\"Argument.metadata\">=</stringProp>\n" + 
    		"                </elementProp>\n" + 
    		"                <elementProp name=\"projection\" elementType=\"Argument\">\n" + 
    		"                  <stringProp name=\"Argument.name\">projection</stringProp>\n" + 
    		"                  <stringProp name=\"Argument.value\">$$PROJECTION$$</stringProp>\n" + 
    		"                  <stringProp name=\"Argument.metadata\">=</stringProp>\n" + 
    		"                </elementProp>\n" + 
    		"                <elementProp name=\"sort\" elementType=\"Argument\">\n" + 
    		"                  <stringProp name=\"Argument.name\">sort</stringProp>\n" + 
    		"                  <stringProp name=\"Argument.value\">$$SORT$$</stringProp>\n" + 
    		"                  <stringProp name=\"Argument.metadata\">=</stringProp>\n" + 
    		"                </elementProp>\n" + 
    		"                <elementProp name=\"limit\" elementType=\"Argument\">\n" + 
    		"                  <stringProp name=\"Argument.name\">limit</stringProp>\n" + 
    		"                  <stringProp name=\"Argument.value\">$$LIMIT$$</stringProp>\n" + 
    		"                  <stringProp name=\"Argument.metadata\">=</stringProp>\n" + 
    		"                </elementProp>" +
    		"                <elementProp name=\"secondary preferred (true|false)\" elementType=\"Argument\">\n" + 
    		"                  <stringProp name=\"Argument.name\">secondary preferred (true|false)</stringProp>\n" + 
    		"                  <stringProp name=\"Argument.value\">false</stringProp>\n" + 
    		"                  <stringProp name=\"Argument.metadata\">=</stringProp>\n" + 
    		"                </elementProp>\n" + 
    		"                  <elementProp name=\"doCount (perform a count query, projection and sort are ignored) (true|false)\" elementType=\"Argument\">\n" + 
    		"                  <stringProp name=\"Argument.name\">doCount (perform a count query, projection and sort are ignored) (true|false)</stringProp>\n" + 
    		"                  <stringProp name=\"Argument.value\">$$DOCOUNT$$</stringProp>\n" + 
    		"                  <stringProp name=\"Argument.metadata\">=</stringProp>\n" + 
    		"                </elementProp>\n";
    
    String jmxThreadClose = "                <elementProp name=\"print cursor data (true|false)\" elementType=\"Argument\">\n" + 
    		"                  <stringProp name=\"Argument.name\">print cursor data (true|false)</stringProp>\n" + 
    		"                  <stringProp name=\"Argument.value\">false</stringProp>\n" + 
    		"                  <stringProp name=\"Argument.metadata\">=</stringProp>\n" + 
    		"                </elementProp>\n" + 
    		"              </collectionProp>\n" + 
    		"            </elementProp>\n" + 
    		"            <stringProp name=\"classname\">com.spireon.jmeter.sampler.SimpleLimitMongoClient</stringProp>\n" + 
    		"          </JavaSampler>\n" + 
    		"          <hashTree>\n" + 
    		"            <CSVDataSet guiclass=\"TestBeanGUI\" testclass=\"CSVDataSet\" testname=\"CSV Data Set Config\" enabled=\"true\">\n" + 
    		"              <stringProp name=\"delimiter\">,</stringProp>\n" + 
    		"              <stringProp name=\"fileEncoding\"></stringProp>\n" + 
    		"              <stringProp name=\"filename\">$$CSVFILE$$</stringProp>\n" + 
    		"              <boolProp name=\"ignoreFirstLine\">false</boolProp>\n" + 
    		"              <boolProp name=\"quotedData\">true</boolProp>\n" + 
    		"              <boolProp name=\"recycle\">true</boolProp>\n" + 
    		"              <stringProp name=\"shareMode\">shareMode.all</stringProp>\n" + 
    		"              <boolProp name=\"stopThread\">false</boolProp>\n" + 
    		"              <stringProp name=\"variableNames\"></stringProp>\n" + 
    		"            </CSVDataSet>\n" + 
    		"            <hashTree/>\n" + 
    		"          </hashTree>\n" +
    		"        </hashTree>\n" +
    		"      </hashTree>\n";
    
    String jmxTestClose = "    </hashTree>\n" + 
    		"  </hashTree>\n" + 
    		"</jmeterTestPlan>";
    
	BufferedWriter jmxFile = null;
	try {
		jmxFile = new BufferedWriter(new FileWriter("test1.jmx"));
		jmxFile.write(jmxHeader);
	} catch (Exception e) {
		System.out.println(e.getMessage());
		return;
	}
    
	
    while (srcCur.hasNext()) {
    	Document thisDoc = srcCur.next();
    	threadCount++;
    	String ns = thisDoc.getString("ns");
    	String filter = thisDoc.getString("Filter");
    	String parseableFilter = filter.replace("[ N ]", "[ 1 ]");
    	Document filterDoc = null;
    	try {
    		filterDoc = Document.parse(parseableFilter);
    	} catch (Exception e) {
    		System.out.println(filter);
    		System.out.println(parseableFilter); 
    		e.printStackTrace();
    	}
    	Set<String> keys = filterDoc.keySet();
    	Iterator<String> keyIterator = keys.iterator();
    	
    	Boolean firstRow = true;

    	Document criteria = new Document("filter_shape", filter).append("Object", ns);
    	MongoCursor<Document> dataCur = dataColl.find(criteria).iterator();
    	
    	if(!dataCur.hasNext()) {    /* Might exists as [ 1 ] */
    		dataCur.close();
    		criteria = new Document("filter_shape", parseableFilter).append("Collection", ns);
    		dataCur = dataColl.find(criteria).iterator();
    		if (!dataCur.hasNext()) {
	    		ObjectId masterID = thisDoc.getObjectId("_id");
	    		System.out.println("No Data for ObjectId('"+masterID.toString()+"')");
	    		System.out.println(criteria);
	    		continue;
    		}
    	}

    	Document dataDoc = dataCur.next();
    	Document attrDoc = dataDoc.get("attr", Document.class);
    	String Sort = null;
    	try {
    		Sort = attrDoc.getString("sort");
    	} catch (Exception e) {
    		Sort = null;
    	}
    	Integer Limit = 0;
    	try {
    		Limit = attrDoc.getInteger("limit");
    	} catch (Exception e) {
    		Limit = null;
    	}
    	
    	String keyName = keyIterator.next();
    	String headerLine = "";
    	String filterString = "{";
    	String csvName = "Thread"+ threadCount.toString()+".csv";
    	String testName = "MongoLoad Test "+ threadCount.toString();
    	BufferedWriter csvFile = null;
    	try {
    		csvFile = new BufferedWriter(new FileWriter(csvName));
    	} catch (Exception e) {
    		System.out.println(e.getMessage());
    		return;
    	}
    	do {
    		@SuppressWarnings("unchecked")
			List<Object> params = (List<Object>)dataDoc.get("filter_params");
    		String param_csv = "";
    		Boolean skipTest = false;
    		for (Object param : params) {
    			Boolean genValue = false;
    			String valVar = null;
    			if (param instanceof String) {
    				String val = (String)param;
        			if (!param_csv.isEmpty())
        				param_csv = param_csv.concat(",");
    				if (val.contains("'"))
    					param_csv = param_csv.concat("\"").concat((String)param).concat("\"");
    				else
    					param_csv = param_csv.concat("'").concat((String)param).concat("'");
    				genValue = true;
    			}
    			else if (param instanceof Integer) {
        			if (!param_csv.isEmpty())
        				param_csv = param_csv.concat(",");
    				param_csv = param_csv.concat(((Integer)param).toString());
    				genValue = true;
    			}
    			else if (param instanceof Long) {
        			if (!param_csv.isEmpty())
        				param_csv = param_csv.concat(",");
    				param_csv = param_csv.concat(((Long)param).toString());
    				genValue = true;
    			}
    			else if (param instanceof ObjectId) {
        			if (!param_csv.isEmpty())
        				param_csv = param_csv.concat(",");
    				param_csv = param_csv.concat("ObjectId('").concat(((ObjectId)param).toString()).concat("')");
    				genValue = true;
    			}
    			else if (param == null) {
        			if (!param_csv.isEmpty())
        				param_csv = param_csv.concat(",");
    				param_csv = param_csv.concat("null");
    				genValue = true;
    			}
    			else if (param instanceof Boolean) {
        			if (!param_csv.isEmpty())
        				param_csv = param_csv.concat(",");
        			if ((Boolean)param)
    					param_csv = param_csv.concat("true");
    				else
    					param_csv = param_csv.concat("false");
    				genValue = true;
    			}
    			else if (param instanceof BsonRegularExpression) {
        			if (!param_csv.isEmpty())
        				param_csv = param_csv.concat(",");
        			BsonRegularExpression regex = (BsonRegularExpression)param;
        			
    				param_csv = param_csv.concat("/"+regex.getPattern()+"/"+regex.getOptions());
    				genValue = true;
    			}
    			else if (param instanceof Date) {
    				if (firstRow) { 
    					if (((Date)param).equals(new Date(253402214400000L))) {
    						valVar = "new Date(253402329540000)";
    					}
    					Instant pTime = ((Date) param).toInstant();
    					Duration offset = Duration.between(pTime,runTime.toInstant());
    					String myOffset = offset.toString();
    					valVar = " new Date(${__timeShift(,,"+myOffset+",,)})";
    					genValue = false;
    				}
    				else continue;
    			}
    			else if (!(param instanceof Date)) {
    				ObjectId probid = (ObjectId)dataDoc.get("_id");
    				System.out.println("Unexpected type: {_id: "+probid.toString()+"}" +param.getClass().getName());
    				if (firstRow) break;
    				continue;
    			}
    			if (firstRow) {
    				if (!filterString.contentEquals("{")) 
    					filterString = filterString.concat(",");
    				if (keyName.contentEquals("$in")) {
    					keyName = keyIterator.next();
        				if (valVar == null) valVar = "${"+keyName+"}";
    					filterString = filterString.concat("&quot;"+keyName+"&quot;: {$in: [ "+valVar+" ] }");
    				}
    				else if (keyName.contentEquals("$and") || keyName.contentEquals("$or")) {
        				ObjectId probid = (ObjectId)dataDoc.get("_id");
        				System.out.println("Unexpected operator: {_id: ObjectId('"+probid.toString()+"')} " +keyName);
        				if (firstRow) {
        					skipTest = true;
        					break;
        				}
        				continue;
    				}
    				else {
        				if (valVar == null) valVar = "${"+keyName+"}";
    					filterString = filterString.concat("&quot;"+keyName+"&quot;: "+valVar);
    				}
    				if (genValue) {
	    				if (!headerLine.isEmpty()) {
	    					headerLine = headerLine.concat(",");
	    				}
	    				headerLine = headerLine.concat(keyName);
    				}
    			}
    			if (firstRow) {
    			    if (keyIterator.hasNext())
    			    	keyName = keyIterator.next();
    			    else
    			    	keyName = "##Missing##";
    			}
    		}
    		if (skipTest) break;
    		if (firstRow) {
    			try {
    				csvFile.write(headerLine+"\n");
    			} catch (Exception e) {
    				System.out.println(e.getMessage());
    			}
    			firstRow = false;
    		}
			try {
				csvFile.write(param_csv+"\n");
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
    		if (dataCur.hasNext())
    			dataDoc = dataCur.next();
    		else 
    			dataDoc = null;
    	} while (dataDoc  != null);
    	try {
			csvFile.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	System.out.println(testName);
    	if (firstRow) continue;
    	try {
			jmxFile.write(jmxThreadHeader.replace("$$TESTNAME$$",testName).replace("$$LOOPNAME$$", testName).replace("$$JAVANAME$$", testName));
			String nsa[] = ns.split("\\.");
			String testDetail = jmxData.replace("$$DB$$",nsa[0]);
			testDetail = testDetail.replace("$$COLLECTION$$", nsa[1]);
			//System.out.println(filterString);
			testDetail = testDetail.replace("$$FILTER$$", filterString+"}");
			testDetail = testDetail.replace("$$PROJECTION$$", "");
			if (Sort == null)
				testDetail = testDetail.replace("$$SORT$$", "");
			else
				testDetail = testDetail.replace("$$SORT$$", Sort);
			if (Limit != null && Limit != 0)
				testDetail = testDetail.replace("$$LIMIT$$", Limit.toString());
			else
				testDetail = testDetail.replace("$$LIMIT$$", "0");
			testDetail = testDetail.replace("$$DOCOUNT$$", "FALSE");
			jmxFile.write(testDetail);
			jmxFile.write(jmxThreadClose.replace("$$CSVFILE$$", csvName));
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    }
    try {
		jmxFile.write(jmxTestClose);
	    jmxFile.close();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
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
	
	static private String fmtArray(ArrayList array, ArrayList parameters) {
		  String arrayShape = "[ ";
		  int scalar = 0;
		  for (Object element : array) {
			  if (element instanceof Document) {
				  if (!arrayShape.contentEquals("[ ")) arrayShape += ",";
				  ArrayList arrParam = new ArrayList();
				  arrayShape += fmtQuery((Document)element,arrParam);
				  if (arrParam instanceof ArrayList) {
					  parameters.addAll(arrParam);
				  } else {
					  parameters.add(arrParam);
				  }
			  } else if (element instanceof ArrayList) { /* Nested Array */
				  if (!arrayShape.contentEquals("[")) arrayShape += ",";
				  ArrayList subParams = new ArrayList();
				  arrayShape += fmtArray((ArrayList)element,subParams);
				  parameters.addAll(subParams);
			  }else { 
				  parameters.add(element);
				  scalar++;			  
			  }
		  }
		  if (scalar > 0) {
			  if (!arrayShape.contentEquals("[ ")) arrayShape += ",";
			  if (scalar == 1)
				  arrayShape += "1";
			  else
				  arrayShape += "N";
		  }
		  arrayShape += " ]";
		  return arrayShape;
		  
	}
	
	static private String fmtQuery(Document filter,ArrayList parameters) {
		Set<String> keys = filter.keySet();
		Iterator<String> iterator = keys.iterator();
		String queryShape = "{";
		while(iterator.hasNext()){
		  if (!queryShape.equals("{")) 
			  queryShape = queryShape.concat(",");
		  String key = iterator.next();
		  if (key.toLowerCase().contentEquals("$nin")) hasNIN=true;
		  if (key.toLowerCase().contentEquals("$ne")) hasNE=true;
//		  if (key.contentEquals("$and")) {
//		  System.out.println("here");
//		  } 
		  Object value = filter.get(key);

		  String valtxt = null;
		  if (value instanceof Document) {
			  ArrayList  subParams = new ArrayList();
			  valtxt = key+": "+fmtQuery((Document)value,subParams);
			  parameters.addAll(subParams);
//			  Set<String> subkeys = ((Document)value).keySet();
//			  Iterator<String> subit = subkeys.iterator();
//			  String subkey = subit.next();

//			  Object subval = ((Document) value).get(subkey);
//			  if (subval instanceof ArrayList) {
//				  parameters.add(subval.toString());
//				  valtxt = key+ ": {"+subkey+": "+"["+String.valueOf(((ArrayList)subval).size())+"]}";
//			  } else if (subval instanceof Document) {
//				  parameters.add("\""+((Document)subval).toString()+"\"");
//				  valtxt = key+ ": {"+subkey+": {}}";
//			  } else {
//				  parameters.add(subval);
//				  valtxt = key+ ": {"+subkey+": 1}";
//			  }
				  
		  } else if (value instanceof ArrayList){
			  ArrayList arrParams = new ArrayList();
			  valtxt = key+": "+fmtArray((ArrayList)value,arrParams);
			  parameters.addAll(arrParams);
		  } else {
			  if (value instanceof ArrayList) {
				  ArrayList arrParams = new ArrayList();
				  valtxt = fmtArray((ArrayList)value,arrParams);
				  parameters.addAll(arrParams);
			  }
			  else
				  parameters.add(value);
			  valtxt = key+": 1";
		  }
		  queryShape = queryShape.concat(valtxt);
		  }
		queryShape = queryShape.concat("}");
		return queryShape;
	}
}
