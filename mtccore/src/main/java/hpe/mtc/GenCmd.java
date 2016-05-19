package hpe.mtc;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import nokia.xml.GranPeriod;
import nokia.xml.MeasCollecFile;
import nokia.xml.MeasInfo;
import nokia.xml.MeasType;
import nokia.xml.MeasValue;
import nokia.xml.R;

public class GenCmd {
	public static final Logger logger = Logger.getLogger(GenCmd.class);
	
	public static final String tablesql_name="tables.sql";
	public static final String copysql_name="copys.sql";
	public static final String schema_name="schemas.txt";
	//used to generate table name
	public static List<String> keyWithValue = new ArrayList<String>();
	public static List<String> keySkip = new ArrayList<String>();
	
	public static void init(){
		keyWithValue.add("PoolType");
		keySkip.add("Machine");
		keySkip.add("UUID");
		keySkip.add("PoolId");
		keySkip.add("PoolMember");
	}
	/*
	 * if -s input-schema not specified, system will 
	 * 		generate input-scheam, table.sql and copy.sql under output-folder
	 * if -s input-schema is specified, and no update detected
	 * 		system will generate multiple csv files, 1 for each type
	 * if new attributes detected, system will generate following files under output-folder
	 * 		updated-input-schema, alter-table.sql, create-table.sql and updated-copy.sql with original names
	 * and backing up the 
	 * 		old input-schema, create-table.sql and copy.sql to history folder with timestamp.
	 */
	public static void usage(){
		System.out.println("GenVsql -s input-schema -i input-xml -o output-folder -p prefix");
	}
	
	public static MeasCollecFile unmarshal(String inputXml){
		try {
			JAXBContext jc = JAXBContext.newInstance("nokia.xml");
		    Unmarshaller u = jc.createUnmarshaller();
		    return (MeasCollecFile) u.unmarshal(new File(inputXml));
		}catch(Exception e){
			logger.error("", e);
			return null;
		}
	}
	
	public static String generateTableName(TreeMap<String, String> moldParams){
		StringBuffer sb = new StringBuffer();
		for (String key: moldParams.keySet()){
			if (!keySkip.contains(key)){
				if (keyWithValue.contains(key)){
					sb.append(String.format("%s_%s", key, moldParams.get(key)));
				}else{
					sb.append(key);
				}
				sb.append("_");
			}
		}
		return sb.toString();
	}
	
	public static void process(MeasCollecFile mf, String outputFolder, String inputSchema, String prefix){
		List<MeasInfo> ml = mf.getMeasData().getMeasInfo();
		LogicSchema logicSchema = new LogicSchema();
		if (inputSchema!=null){
			logicSchema = LogicSchema.fromFile(inputSchema);
		}
		boolean schemaUpdated=false;
		Map<String, List<String>> schemaAttrNameUpdates = new HashMap<String, List<String>>();//store updated attribute parts' name (compared with the org schema)
		Map<String, List<String>> schemaAttrTypeUpdates = new HashMap<String, List<String>>();//store updated attribute parts' type (compared with the org schema)
		Map<String, List<String>> objNameAdded = new HashMap<String, List<String>>();
		Map<String, List<String>> objTypeAdded = new HashMap<String, List<String>>();
		for (MeasInfo mi:ml){
			MeasValue mv0 = mi.getMeasValue().get(0);
			String moldn = mv0.getMeasObjLdn();
			TreeMap<String, String> moldParams = Util.parseMapParams(moldn);
			String tableName = generateTableName(moldParams);
			List<String> orgSchemaAttributes = logicSchema.getAttributes(tableName);
			{//merge the origin and newUpdates
				List<String> newSchemaAttributes = schemaAttrNameUpdates.get(tableName);
				if (newSchemaAttributes!=null){
					if (orgSchemaAttributes == null)
						orgSchemaAttributes = newSchemaAttributes;
					else{
						orgSchemaAttributes.addAll(newSchemaAttributes);
					}
				}
			}
			if (orgSchemaAttributes!=null){
				List<String> mtcl = new ArrayList<String>();
				for (MeasType mt: mi.getMeasType()){
					mtcl.add(mt.getContent());
				}
				//check new attribute
				List<String> newAttrNames = new ArrayList<String>();
				List<String> newAttrTypes = new ArrayList<String>();
				for (int i=0; i<mtcl.size(); i++){
					String mtc = mtcl.get(i);
					if (!orgSchemaAttributes.contains(mtc)){
						newAttrNames.add(mtc);
						newAttrTypes.add(Util.guessType(mv0.getR().get(i).getContent()));
					}
				}
				if (newAttrNames.size()>0){
					if (schemaAttrNameUpdates.containsKey(tableName)){
						newAttrNames.addAll(0, schemaAttrNameUpdates.get(tableName));
						newAttrTypes.addAll(0, schemaAttrTypeUpdates.get(tableName));
					}
					schemaAttrNameUpdates.put(tableName, newAttrNames);
					schemaAttrTypeUpdates.put(tableName, newAttrTypes);
					schemaUpdated=true;
				}else{
					if (!schemaUpdated){//gen data
						GranPeriod gp = mi.getGranPeriod();
						//gen value idx mapping
						Map<Integer,Integer> mapping = new HashMap<Integer, Integer>();//current attribute maps to which schema attribute
						for (int i=0; i<orgSchemaAttributes.size(); i++){
							String attr = orgSchemaAttributes.get(i);
							int idx = mtcl.indexOf(attr);
							if (idx!=-1){
								mapping.put(idx, i);
							}
						}
						List<List<String>> fieldValueLists = new ArrayList<List<String>>();
						for (MeasValue mvl:mi.getMeasValue()){
							List<String> fieldValues = new ArrayList<String>();
							fieldValues.add(gp.getEndTime());
							fieldValues.add(gp.getDuration());
							TreeMap<String, String> kvs = Util.parseMapParams(mvl.getMeasObjLdn());
							for (String v:kvs.values()){
								fieldValues.add(v);
							}
							String[] vs = new String[orgSchemaAttributes.size()];
							for (int i=0; i<mvl.getR().size(); i++){
								String v = mvl.getR().get(i).getContent();
								vs[mapping.get(i)]=v;
							}
							fieldValues.addAll(Arrays.asList(vs));
							fieldValueLists.add(fieldValues);
						}
					}
				}
			}else{
				//update schemaAttrName/TypeUpdates
				List<String> onlyAttrNamesList = new ArrayList<String>();
				for (MeasType mt: mi.getMeasType()){
					onlyAttrNamesList.add(mt.getContent());
				}
				List<String> onlyAttrTypesList = new ArrayList<String>();
				for (R r: mv0.getR()){
					onlyAttrTypesList.add(Util.guessType(r.getContent()));
				}
				schemaAttrNameUpdates.put(tableName, onlyAttrNamesList);
				schemaAttrTypeUpdates.put(tableName, onlyAttrTypesList);
				//
				List<String> objNameList = new ArrayList<String>();
				List<String> objTypeList = new ArrayList<String>();
				for (String key: moldParams.keySet()){
					objNameList.add(key);
					objTypeList.add(Util.guessType(moldParams.get(key)));
				}
				objNameAdded.put(tableName, objNameList);
				objTypeAdded.put(tableName, objTypeList);
				schemaUpdated=true;
			}
		}
		
		//generate
		String tablesql = null;
		String copysql = null;
		if (schemaUpdated){
			for(String tn:schemaAttrNameUpdates.keySet()){
				if (logicSchema.getAttributes(tn)!=null){
					//update
				}else{
					//create
					List<String> fieldNameList = new ArrayList<String>(); 
					List<String> fieldTypeList = new ArrayList<String>();
					//adding sys fields
					fieldNameList.add("endTime");
					fieldNameList.add("duration");
					fieldTypeList.add("TIMESTAMP not null");
					fieldTypeList.add("int");
					//adding obj fields
					fieldNameList.addAll(objNameAdded.get(tn));
					fieldTypeList.addAll(objTypeAdded.get(tn));
					//adding attr fields
					fieldNameList.addAll(schemaAttrNameUpdates.get(tn));
					fieldTypeList.addAll(schemaAttrTypeUpdates.get(tn));
					tablesql = Util.genCreateTableSql(fieldNameList, fieldTypeList, tn);
				}
				//update to logic schema
				logicSchema.updateOrAddAttributes(tn, schemaAttrNameUpdates.get(tn));
			}
			//gen copy sql
			
			//gen schema
			
		}else{
			//gen data.csv
		}
		//gen files
		String outputPrefix = prefix==null? outputFolder:outputFolder+prefix;
		try {
			if (schemaUpdated){
				FileUtils.writeStringToFile(new File(outputPrefix + tablesql_name), tablesql, Charset.defaultCharset());
			}
		}catch(Exception e){
			logger.error("", e);
		}
	}
	
	public static void main(String[] args){
		init();
		CommandLine commandLine;
		Option optionInput = Option.builder("i").hasArgs().required(true).longOpt("input-xml").build();
		Option optionOutput = Option.builder("o").hasArgs().required(true).longOpt("output-folder").build();
		Option optionSchema = Option.builder("s").hasArgs().longOpt("input-schema").build();
		Option optionPrefix = Option.builder("p").hasArgs().longOpt("prefix").build();
		Options options = new Options();
		options.addOption(optionInput);
		options.addOption(optionOutput);
		options.addOption(optionSchema);
		options.addOption(optionPrefix);
		CommandLineParser parser = new DefaultParser();
		try{
			commandLine = parser.parse(options, args);
			String inputXml = commandLine.getOptionValue("i");
			String outputFolder = commandLine.getOptionValue("o");
			String inputSchema = null;
			if (commandLine.hasOption("s")){
				inputSchema = commandLine.getOptionValue("s");
			}
			String prefix = null;
			if (commandLine.hasOption("p")){
				prefix = commandLine.getOptionValue("p");
			}
			MeasCollecFile mf = unmarshal(inputXml);
			process(mf, outputFolder, inputSchema, prefix);
			
		}catch (ParseException exception){
			usage();
		}
	}
}
