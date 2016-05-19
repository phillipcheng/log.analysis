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
	public static List<String> keyWithValue = new ArrayList<String>();
	public static List<String> keySkip = new ArrayList<String>();
	
	public static void init(){
		keyWithValue.add("PoolType");
		keySkip.add("Machine");
		keySkip.add("UUID");
	}
	/*
	 * if -s input-schema not specified, system will 
	 * 		generate input-scheam, table.sql and copy.sql under output-folder
	 * if -s input-schema is specified, system will generate multiple csv files, 1 for each type
	 * if new attributes detected, system will generate following files under output-folder
	 * 		updated-input-schema, alter-table.sql and updated-copy.sql with original names
	 * and backing up the 
	 * 		old input-schema, table.sql and copy.sql to history folder with timestamp.
	 */
	public static void usage(){
		System.out.println("GenVsql -s input-schema -i input-xml -o output-folder");
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
	
	public static String guessType(String value){
		boolean atleastOneAlpha = value.matches(".*[a-zA-Z]+.*");
		if (atleastOneAlpha){
			int len = value.length();
			return String.format("varchar(%d)", Math.max(20, 2*len));
		}else{
			return String.format("numeric(%d,%d)", 15,5);
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
	
	public static void process(MeasCollecFile mf, String outputPrefix, String inputSchema){
		List<MeasInfo> ml = mf.getMeasData().getMeasInfo();
		StringBuffer tablesql = new StringBuffer();
		StringBuffer copysql = new StringBuffer();
		LogicSchema ls = new LogicSchema();
		if (inputSchema!=null){
			ls = LogicSchema.fromFile(inputSchema);
		}
		boolean schemaUpdated=false;
		Map<String, List<String>> schemaAttrNameUpdates = new HashMap<String, List<String>>();
		Map<String, List<String>> schemaAttrTypeUpdates = new HashMap<String, List<String>>();
		for (MeasInfo mi:ml){
			MeasValue mv0 = mi.getMeasValue().get(0);
			String moldn = mv0.getMeasObjLdn();
			TreeMap<String, String> moldParams = Util.parseMapParams(moldn);
			String tableName = generateTableName(moldParams);
			List<String> schemaAttributes = ls.getAttributes(tableName);
			GranPeriod gp = mi.getGranPeriod();
			
			if (schemaAttributes!=null){
				List<String> mtcl = new ArrayList<String>();
				for (MeasType mt: mi.getMeasType()){
					mtcl.add(mt.getContent());
				}
				//check new attribute
				List<String> newAttrNames = new ArrayList<String>();
				List<String> newAttrTypes = new ArrayList<String>();
				for (int i=0; i<mtcl.size(); i++){
					String mtc = mtcl.get(i);
					if (!schemaAttributes.contains(mtc)){
						newAttrNames.add(mtc);
						newAttrTypes.add(guessType(mv0.getR().get(i).getContent()));
					}
				}
				if (newAttrNames.size()>0){
					schemaAttributes.addAll(newAttrNames);
					if (schemaAttrNameUpdates.containsKey(tableName)){
						newAttrNames.addAll(schemaAttrNameUpdates.get(tableName));
						newAttrTypes.addAll(schemaAttrTypeUpdates.get(tableName));
					}
					schemaAttrNameUpdates.put(tableName, newAttrNames);
					schemaAttrTypeUpdates.put(tableName, newAttrTypes);
					ls.updateOrAddAttributes(tableName, schemaAttributes);
					schemaUpdated=true;
				}else{
					if (!schemaUpdated){//gen data
						//gen value idx mapping
						Map<Integer,Integer> mapping = new HashMap<Integer, Integer>();//current attribute maps to which schema attribute
						for (int i=0; i<schemaAttributes.size(); i++){
							String attr = schemaAttributes.get(i);
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
							String[] vs = new String[schemaAttributes.size()];
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
				List<String> newAttrNamesList = new ArrayList<String>();
				newAttrNamesList.add("endTime");
				newAttrNamesList.add("duration");
				List<String> newAttrTypesList = new ArrayList<String>();
				newAttrTypesList.add("TIMESTAMP not null");
				newAttrTypesList.add("int");
				for (MeasType mt: mi.getMeasType()){
					newAttrNamesList.add(mt.getContent());
				}
				for (String key: moldParams.keySet()){
					newAttrNamesList.add(key);
					newAttrTypesList.add(guessType(moldParams.get(key)));
				}
				for (R r: mv0.getR()){
					newAttrTypesList.add(guessType(r.getContent()));
				}
				schemaAttrNameUpdates.put(tableName, newAttrNamesList);
				schemaAttrTypeUpdates.put(tableName, newAttrTypesList);
				ls.updateOrAddAttributes(tableName, schemaAttributes);
				schemaUpdated=true;
			}
		}
		
		//generate
		if (schemaUpdated){
			//gen tables.sql and copy.sql
			for(String tn:schemaAttrNameUpdates.keySet()){
				if (ls.getAttributes(tn)!=null){
					//update
				}else{
					//create
					List<String> fieldNameList = schemaAttrNameUpdates.get(tn);
					List<String> fieldTypeList = schemaAttrTypeUpdates.get(tn);
					for (int i=0; i<fieldNameList.size(); i++){
						String name = fieldNameList.get(i);
						if (name.contains(".")){
							name = name.substring(name.lastIndexOf(".")+1);
							fieldNameList.set(i, name);
						}
					}
					//gen table sql
					tablesql.append(String.format("drop table %s;\n", tn));
					tablesql.append(String.format("create table if not exists %s(\n", tn));
					for (int i=0; i<fieldNameList.size(); i++){
						String name = fieldNameList.get(i);
						String type = fieldTypeList.get(i);
						tablesql.append(String.format("%s %s", name, type));
						if (i<fieldNameList.size()-1){
							tablesql.append(",");
						}
					}
					tablesql.append(");\n");
				}
			}
			//get copy sql
		}else{
			//gen data.csv
		}
		
		try {
			FileUtils.writeStringToFile(new File(outputPrefix + tablesql_name), tablesql.toString(), Charset.defaultCharset());
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
		Options options = new Options();
		options.addOption(optionInput);
		options.addOption(optionOutput);
		options.addOption(optionSchema);
		CommandLineParser parser = new DefaultParser();
		try{
			commandLine = parser.parse(options, args);
			String inputXml = commandLine.getOptionValue("i");
			String outputFolder = commandLine.getOptionValue("o");
			String inputSchema = null;
			if (commandLine.hasOption("s")){
				inputSchema = commandLine.getOptionValue("s");
			}
			MeasCollecFile mf = unmarshal(inputXml);
			process(mf, outputFolder, inputSchema);
			
		}catch (ParseException exception){
			usage();
		}
	}
}
