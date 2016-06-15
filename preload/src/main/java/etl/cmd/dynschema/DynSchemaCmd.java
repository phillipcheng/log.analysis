package etl.cmd.dynschema;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import etl.engine.ETLCmd;
import etl.util.Util;

public class DynSchemaCmd extends ETLCmd{
	public static final Logger logger = Logger.getLogger(DynSchemaCmd.class);
	
	public static final String schema_name="schemas.txt";
	
	public static final String cfgkey_hdfs_defaultfs="hdfs.defaultfs";
	public static final String cfgkey_xml_folder="xml-folder";
	public static final String cfgkey_csv_folder="csv-folder";
	public static final String cfgkey_schema_folder="schema-folder";
	public static final String cfgkey_schema_history_folder="schema-history-folder";
	public static final String cfgkey_data_history_folder="data-history-folder";
	public static final String cfgkey_prefix="prefix";
	
	public static final String cfgkey_FileLvelSystemAttrs_xpath="FileSystemAttrs.xpath";
	public static final String cfgkey_FileLvelSystemAttrs_name="FileSystemAttrs.name";
	public static final String cfgkey_FileLvelSystemAttrs_type="FileSystemAttrs.type";
	
	public static final String cfgkey_xpath_Tables="xpath.Tables";
	public static final String cfgkey_xpath_TableRow0="xpath.TableRow0";
	public static final String cfgkey_xpath_TableObjDesc="xpath.TableObjDesc";
	public static final String cfgkey_xpath_TableAttrNames="xpath.TableAttrNames";
	public static final String cfgkey_xpath_TableRows="xpath.TableRows";
	public static final String cfgkey_xpath_TableRowValues="xpath.TableRowValues";
	
	public static final String cfgkey_TableSystemAttrs_xpath="TableSystemAttrs.xpath";
	public static final String cfgkey_TableSystemAttrs_name="TableSystemAttrs.name";
	public static final String cfgkey_TableSystemAttrs_type="TableSystemAttrs.type";
	
	
	public static final String createtablesql_name="createtables.sql";
	public static final String droptablesql_name="droptables.sql";
	public static final String trunctablesql_name="trunctables.sql";
	public static final String copysql_name="copys.sql";
	
	public static final String dynCfg_Key_TABLES_USED="tables.used";
	public static final String dynCfg_Key_CREATETABLE_SQL_FILE="create.table.sql.file";
	public static final String dynCfg_Key_XML_FILES="raw.xml.files";
	
	//used to generate table name
	private List<String> keyWithValue = new ArrayList<String>();
	private List<String> keySkip = new ArrayList<String>();
	private List<String> fileLvlSystemFieldNames = new ArrayList<String>();
	private List<String> fileLvlSystemFieldTypes = new ArrayList<String>();
	private List<String> tableLvlSystemFieldNames = new ArrayList<String>();
	private List<String> tableLvlSystemFieldTypes = new ArrayList<String>();
	
	private String xmlFolder;
	private String csvFolder;
	private String schemaHistoryFolder;
	private String prefix;//used as dbschema name
	private String schemaFileName;
	private LogicSchema logicSchema;
	//
	private XPathExpression FileLvlSystemAttrsXpath;//file level
	private XPathExpression xpathExpTables;
	private XPathExpression xpathExpTableRow0;
	private XPathExpression xpathExpTableObjDesc;
	private XPathExpression xpathExpTableAttrNames;
	private XPathExpression xpathExpTableRows;
	private XPathExpression[] xpathExpTableSystemAttrs;//table level
	private XPathExpression xpathExpTableRowValues;
	//
	private Map<String, List<String>> schemaAttrNameUpdates = new HashMap<String, List<String>>();//store updated/new tables' attribute parts' name (compared with the org schema)
	private Map<String, List<String>> schemaAttrTypeUpdates = new HashMap<String, List<String>>();//store updated/new tables' attribute parts' type (compared with the org schema)
	private Map<String, List<String>> newTableObjNamesAdded = new HashMap<String, List<String>>();//store new tables' obj name
	private Map<String, List<String>> newTableObjTypesAdded = new HashMap<String, List<String>>();//store new tables' obj type
	private Map<String, BufferedWriter> fvWriterMap = new HashMap<String, BufferedWriter>();//store all the data files generated, key by file name
	private Set<String> tablesUsed = new HashSet<String>(); //the tables this batch of data used
	
	public void setup(PropertiesConfiguration pc){
		keyWithValue.add("PoolType");
		
		keySkip.add("Machine");
		keySkip.add("UUID");
		keySkip.add("PoolId");
		keySkip.add("PoolMember");
		
		//
		XPathFactory xPathfactory = XPathFactory.newInstance();
		XPath xpath = xPathfactory.newXPath();
		try {
			
			FileLvlSystemAttrsXpath = xpath.compile(pc.getString(cfgkey_FileLvelSystemAttrs_xpath));
			for (String name: pc.getStringArray(cfgkey_FileLvelSystemAttrs_name)){
				fileLvlSystemFieldNames.add(name);
			}
			for (String type: pc.getStringArray(cfgkey_FileLvelSystemAttrs_type)){
				fileLvlSystemFieldTypes.add(type);
			}
			xpathExpTables = xpath.compile(pc.getString(cfgkey_xpath_Tables));
			xpathExpTableRow0 = xpath.compile(pc.getString(cfgkey_xpath_TableRow0));
			xpathExpTableObjDesc = xpath.compile(pc.getString(cfgkey_xpath_TableObjDesc));
			xpathExpTableAttrNames = xpath.compile(pc.getString(cfgkey_xpath_TableAttrNames));
			xpathExpTableRows = xpath.compile(pc.getString(cfgkey_xpath_TableRows));
			xpathExpTableRowValues = xpath.compile(pc.getString(cfgkey_xpath_TableRowValues));
			String[] tsxpaths = pc.getStringArray(cfgkey_TableSystemAttrs_xpath);
			xpathExpTableSystemAttrs = new XPathExpression[tsxpaths.length];
			for (int i=0; i<tsxpaths.length; i++){
				xpathExpTableSystemAttrs[i]=xpath.compile(tsxpaths[i]);
			}
			for (String name: pc.getStringArray(cfgkey_TableSystemAttrs_name)){
				tableLvlSystemFieldNames.add(name);
			}
			for (String type: pc.getStringArray(cfgkey_TableSystemAttrs_type)){
				tableLvlSystemFieldTypes.add(type);
			}
			this.xmlFolder = pc.getString(cfgkey_xml_folder);
			this.csvFolder = pc.getString(cfgkey_csv_folder);
			this.schemaHistoryFolder = pc.getString(cfgkey_schema_history_folder);
			this.prefix = pc.getString(cfgkey_prefix);
			this.schemaFileName = pc.getString(cfgkey_schema_folder) + prefix +"." + schema_name;
			this.logicSchema = new LogicSchema();
			Path schemaFile = new Path(schemaFileName);
			if (fs.exists(schemaFile)){
				logicSchema = (LogicSchema) Util.fromDfsFile(fs, schemaFileName, LogicSchema.class);
			}
		}catch(Exception e){
			logger.info("", e);
		}
	}
	
	public DynSchemaCmd(String wfid, String staticCfg, String inDynCfg, String outDynCfg, String defaultFs){
		super(wfid, staticCfg, inDynCfg, outDynCfg, defaultFs);
		setup(pc);
	}
	
	
	
	public Document getDocument(FileStatus inputXml){
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			InputSource input = new InputSource(new BufferedReader(new InputStreamReader(fs.open(inputXml.getPath()))));
			Document doc = builder.parse(input);
			return doc;
		}catch(Exception e){
			logger.error("", e);
			return null;
		}
	}
	
	public String generateTableName(TreeMap<String, String> moldParams){
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
	
	public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	private void closeCsvWriters(){
		for (BufferedWriter bw:fvWriterMap.values()){
			if (bw!=null){
				try {
					bw.close();
				}catch(Exception e){
					logger.error("", e);
				}
			}
		}
	}
	private String getOutputDataFileName(String tableName, String timeStr){
		return csvFolder + timeStr + "_" + tableName + ".csv";
	}

	private Node getNode(NodeList nl, int idx){
		Node n = nl.item(idx);
		n.getParentNode().removeChild(n);//for performance
		return n;
	}
	
	private void genData(Node mi, Map<String, String> localDnMap, List<String> orgSchemaAttributes, List<String> mtcl, String tableName, 
			Map<String, BufferedWriter> fvWriterMap, String startProcessTime) throws Exception {
		List<String> tableLvlSystemAttValues = new ArrayList<String>();
		for (XPathExpression exp:xpathExpTableSystemAttrs){
			tableLvlSystemAttValues.add((String) exp.evaluate(mi, XPathConstants.STRING));
		}
		//gen value idx mapping
		Map<Integer,Integer> mapping = new HashMap<Integer, Integer>();//current attribute maps to which schema attribute
		for (int i=0; i<orgSchemaAttributes.size(); i++){
			String attr = orgSchemaAttributes.get(i);
			int idx = mtcl.indexOf(attr);
			if (idx!=-1){
				mapping.put(idx, i);
			}
		}
		NodeList mvl = (NodeList) xpathExpTableRows.evaluate(mi, XPathConstants.NODESET);
		for (int k=0; k<mvl.getLength(); k++){
			Node mv = getNode(mvl, k);
			List<String> fieldValues = new ArrayList<String>();
			//system values
			for (String v:tableLvlSystemAttValues){
				fieldValues.add(v);
			}
			for (String n:fileLvlSystemFieldNames){
				fieldValues.add(localDnMap.get(n));	
			}
			//object values
			String moldn = (String) xpathExpTableObjDesc.evaluate(mv, XPathConstants.STRING);
			TreeMap<String, String> kvs = Util.parseMapParams(moldn);
			for (String v:kvs.values()){
				fieldValues.add(v);
			}
			String[] vs = new String[orgSchemaAttributes.size()];
			NodeList rlist = (NodeList) xpathExpTableRowValues.evaluate(mv, XPathConstants.NODESET);
			for (int i=0; i<rlist.getLength(); i++){
				Node r = getNode(rlist, i);
				String v = r.getTextContent();
				vs[mapping.get(i)]=v;
			}
			fieldValues.addAll(Arrays.asList(vs));
			String outputFileName = getOutputDataFileName(tableName, startProcessTime);
			BufferedWriter osw = fvWriterMap.get(outputFileName);
			if (osw ==null) {
				osw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(outputFileName))));
				fvWriterMap.put(outputFileName, osw);
			}
			String csv = Util.getCsv(fieldValues);
			osw.write(csv);
		}
	}
	
	private boolean checkSchemaUpdateOrGenData(FileStatus[] inputFileNames, String strProcessStartTime){
		boolean schemaUpdated=false;
		try {
			for (FileStatus inputFile: inputFileNames){
				logger.debug(String.format("process %s", inputFile));
				Document mf = getDocument(inputFile);
				Map<String, String> localDnMap = Util.parseMapParams((String)FileLvlSystemAttrsXpath.evaluate(mf, XPathConstants.STRING));
				NodeList ml = (NodeList) xpathExpTables.evaluate(mf, XPathConstants.NODESET);
				for (int i=0; i<ml.getLength(); i++){
					Node mi = getNode(ml, i);
					Node mv0 = (Node)xpathExpTableRow0.evaluate(mi, XPathConstants.NODE);
					String moldn = (String) xpathExpTableObjDesc.evaluate(mv0, XPathConstants.STRING);
					TreeMap<String, String> moldParams = Util.parseMapParams(moldn);
					String tableName = generateTableName(moldParams);
					tablesUsed.add(tableName);
					NodeList mv0vs = (NodeList) xpathExpTableRowValues.evaluate(mv0, XPathConstants.NODESET);
					List<String> orgSchemaAttributes = null;
					if (logicSchema.hasAttrNames(tableName)){
						orgSchemaAttributes = new ArrayList<String>();
						orgSchemaAttributes.addAll(logicSchema.getAttrNames(tableName));
					}
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
					List<String> tableAttrNamesList = new ArrayList<String>();//table attr name list
					NodeList mts = (NodeList) xpathExpTableAttrNames.evaluate(mi, XPathConstants.NODESET);
					for (int j=0; j<mts.getLength(); j++){
						Node mt = getNode(mts, j);
						tableAttrNamesList.add(mt.getTextContent());
					}
					if (orgSchemaAttributes!=null){
						//check new attribute
						List<String> newAttrNames = new ArrayList<String>();
						List<String> newAttrTypes = new ArrayList<String>();
						for (int j=0; j<tableAttrNamesList.size(); j++){
							String mtc = tableAttrNamesList.get(j);
							if (!orgSchemaAttributes.contains(mtc)){
								newAttrNames.add(mtc);
								Node mv0vj = getNode(mv0vs, j);
								newAttrTypes.add(Util.guessType(mv0vj.getTextContent()));
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
								genData(mi, localDnMap, orgSchemaAttributes, tableAttrNamesList, tableName, fvWriterMap, strProcessStartTime);
							}
						}
					}else{
						//new table needed
						List<String> onlyAttrTypesList = new ArrayList<String>();
						for (int j=0; j<mv0vs.getLength(); j++){
							Node mv0vj = getNode(mv0vs, j);
							onlyAttrTypesList.add(Util.guessType(mv0vj.getTextContent()));
						}
						schemaAttrNameUpdates.put(tableName, tableAttrNamesList);
						schemaAttrTypeUpdates.put(tableName, onlyAttrTypesList);
						//
						List<String> objNameList = new ArrayList<String>();
						List<String> objTypeList = new ArrayList<String>();
						for (String key: moldParams.keySet()){
							objNameList.add(key);
							objTypeList.add(Util.guessType(moldParams.get(key)));
						}
						newTableObjNamesAdded.put(tableName, objNameList);
						newTableObjTypesAdded.put(tableName, objTypeList);
						schemaUpdated=true;
					}
				}
			}
		}catch(Exception e){
			logger.error("", e);
		}finally{
			closeCsvWriters();
		}
		return schemaUpdated;
	}
	
	/**
	output: schema, createsql, csvfiles. if updated schema detected
		  : csvfiles. if no updated schema
		  : dynCfgOutput${wf.id} with contents:
		  		create-sql-file-name:xxx
		  		xml-file-names:
		  		used-tables: (used to generate csv file names)
	*/
	@Override
	public List<String> process(String param, Mapper<Object, Text, Text, NullWritable>.Context context) {
		try {
			Map<String, List<String>> dynCfgOutput = new HashMap<String, List<String>>();
			String createsqlFileName = String.format("%s%s.%s_%s", schemaHistoryFolder, prefix, createtablesql_name, wfid);
			FileStatus[] inputFileNames = fs.listStatus(new Path(xmlFolder));
			//1. check new/update schema, generate csv files
			boolean schemaUpdated = checkSchemaUpdateOrGenData(inputFileNames, wfid);
			//generate schema files
			List<String> createTableSqls = new ArrayList<String>();
			if (schemaUpdated){
				List<String> dropTableSqls = new ArrayList<String>();
				List<String> truncTableSqls = new ArrayList<String>();
				List<String> copysqls = new ArrayList<String>();
				for(String tn:schemaAttrNameUpdates.keySet()){
					List<String> fieldNameList = new ArrayList<String>(); 
					List<String> fieldTypeList = new ArrayList<String>();
					if (logicSchema.getAttrNames(tn)!=null){//update table
						//gen update sql
						fieldNameList.addAll(schemaAttrNameUpdates.get(tn));
						fieldTypeList.addAll(schemaAttrTypeUpdates.get(tn));
						createTableSqls.addAll(Util.genUpdateTableSql(fieldNameList, fieldTypeList, tn, prefix));
						logicSchema.addAttributes(tn, schemaAttrNameUpdates.get(tn));
					}else{//new table
						//update to logic schema
						logicSchema.addAttributes(tn, schemaAttrNameUpdates.get(tn));
						logicSchema.addObjParams(tn, newTableObjNamesAdded.get(tn));
						//gen create sql
						fieldNameList.addAll(tableLvlSystemFieldNames);
						fieldNameList.addAll(fileLvlSystemFieldNames);
						fieldTypeList.addAll(tableLvlSystemFieldTypes);
						fieldTypeList.addAll(fileLvlSystemFieldTypes);
						fieldNameList.addAll(newTableObjNamesAdded.get(tn));
						fieldTypeList.addAll(newTableObjTypesAdded.get(tn));
						fieldNameList.addAll(schemaAttrNameUpdates.get(tn));
						fieldTypeList.addAll(schemaAttrTypeUpdates.get(tn));
						createTableSqls.add(Util.genCreateTableSql(fieldNameList, fieldTypeList, tn, prefix));
						dropTableSqls.add(Util.genDropTableSql(tn, prefix));
						truncTableSqls.add(Util.genTruncTableSql(tn, prefix));
					}
					//gen copys.sql for reference
					fieldNameList.clear();
					fieldNameList.addAll(tableLvlSystemFieldNames);
					fieldNameList.addAll(fileLvlSystemFieldNames);
					fieldNameList.addAll(logicSchema.getObjParams(tn));
					fieldNameList.addAll(logicSchema.getAttrNames(tn));
					String copySql = Util.genCopyLocalSql(fieldNameList, tn, prefix, getOutputDataFileName(tn, wfid));
					copysqls.add(copySql);
				}
				
				
				//gen schema files to history dir for reference
				Util.writeDfsFile(fs, createsqlFileName, createTableSqls);
				Util.writeDfsFile(fs, String.format("%s%s.%s_%s", schemaHistoryFolder, prefix, droptablesql_name, wfid), dropTableSqls);
				Util.writeDfsFile(fs, String.format("%s%s.%s_%s", schemaHistoryFolder, prefix, trunctablesql_name, wfid), truncTableSqls);
				Util.writeDfsFile(fs, String.format("%s%s.%s_%s", schemaHistoryFolder, prefix, copysql_name, wfid), copysqls);
				
				//gen logic schema
				Util.toDfsFile(fs, schemaFileName, logicSchema);
				
				//gen data again using new schema
				fvWriterMap.clear();
				schemaAttrNameUpdates.clear();
				checkSchemaUpdateOrGenData(inputFileNames, wfid);
				//only generate createtable sql entry when there is schema update
				List<String> csfn = new ArrayList<String>();
				csfn.add(createsqlFileName);
				dynCfgOutput.put(dynCfg_Key_CREATETABLE_SQL_FILE, csfn);
			}
			
			List<String> utl = new ArrayList<String>();
			List<String> rawFiles = new ArrayList<String>();
			utl.addAll(tablesUsed);
			for (FileStatus inputFileName:inputFileNames){
				rawFiles.add(inputFileName.getPath().getName());
			}
			dynCfgOutput.put(dynCfg_Key_TABLES_USED, utl);
			dynCfgOutput.put(dynCfg_Key_XML_FILES, rawFiles);
			Util.toDfsFile(fs, this.outDynCfg, dynCfgOutput);
		}catch(Exception e){
			logger.error("", e);
		}
		return null;
	}
}
