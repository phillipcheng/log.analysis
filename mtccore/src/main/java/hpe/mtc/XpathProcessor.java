package hpe.mtc;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
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

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class XpathProcessor {
	public static final Logger logger = Logger.getLogger(XpathProcessor.class);
	
	public static final String schema_name="schemas.txt";
	
	public static final String cfgkey_xml_folder="xml-folder";
	public static final String cfgkey_csv_folder="csv-folder";
	public static final String cfgkey_schema_folder="schema-folder";
	public static final String cfgkey_schema_history_folder="schema-history-folder";
	public static final String cfgkey_data_history_folder="data-history-folder";
	public static final String cfgkey_prefix="prefix";
	
	
	public static final String createtablesql_name="createtables.sql";
	public static final String droptablesql_name="droptables.sql";
	public static final String trunctablesql_name="trunctables.sql";
	public static final String copysql_name="copys.sql";
	
	public static final String SYSTEM_FIELD_ENDTIME="endTime";
	public static final String SYSTEM_FIELD_SUBNETWORK="SubNetwork";
	public static final String SYSTEM_FIELD_MANAGEDELEMENT="ManagedElement";
	
	//used to generate table name
	private List<String> keyWithValue = new ArrayList<String>();
	private List<String> keySkip = new ArrayList<String>();
	private List<String> systemFieldNames = new ArrayList<String>();
	private List<String> systemFieldTypes = new ArrayList<String>();
	
	private PropertiesConfiguration pc;
	private String xmlFolder;
	private String csvFolder;
	private String schemaHistoryFolder;
	private String dataHistoryFolder;
	private String prefix;//used as dbschema name
	private String schemaFileName;
	private LogicSchema logicSchema;
	//
	private XPathExpression xpathExpFileSystemAttr1;//file level
	private XPathExpression xpathExpTables;
	private XPathExpression xpathExpTableRow0;
	private XPathExpression xpathExpTableObjDesc;
	private XPathExpression xpathExpTableAttrNames;
	private XPathExpression xpathExpTableRows;
	private XPathExpression xpathExpTableSystemAttr1;//table level
	private XPathExpression xpathExpTableSystemAttr2;//table level
	private XPathExpression xpathExpTableRowValues;
	//
	private Map<String, List<String>> schemaAttrNameUpdates = new HashMap<String, List<String>>();//store updated/new tables' attribute parts' name (compared with the org schema)
	private Map<String, List<String>> schemaAttrTypeUpdates = new HashMap<String, List<String>>();//store updated/new tables' attribute parts' type (compared with the org schema)
	private Map<String, List<String>> newTableObjNamesAdded = new HashMap<String, List<String>>();//store new tables' obj name
	private Map<String, List<String>> newTableObjTypesAdded = new HashMap<String, List<String>>();//store new tables' obj type
	private Map<String, BufferedWriter> fvWriterMap = new HashMap<String, BufferedWriter>();//store all the data files generated, key by file name
	private Set<String> tablesUsed = new HashSet<String>(); //the tables this batch of data used
	
	public XpathProcessor(PropertiesConfiguration pc){
		this.pc = pc;
		
		keyWithValue.add("PoolType");
		
		keySkip.add("Machine");
		keySkip.add("UUID");
		keySkip.add("PoolId");
		keySkip.add("PoolMember");
		
		systemFieldNames.add(SYSTEM_FIELD_ENDTIME);
		systemFieldNames.add("duration");
		systemFieldNames.add(SYSTEM_FIELD_SUBNETWORK);
		systemFieldNames.add(SYSTEM_FIELD_MANAGEDELEMENT);
		
		systemFieldTypes.add("TIMESTAMP not null");
		systemFieldTypes.add("varchar(10)");
		systemFieldTypes.add("varchar(70)");
		systemFieldTypes.add("varchar(70)");
		//
		XPathFactory xPathfactory = XPathFactory.newInstance();
		XPath xpath = xPathfactory.newXPath();
		try {
			xpathExpFileSystemAttr1 = xpath.compile("/measCollecFile/fileHeader/fileSender/@localDn");
			xpathExpTables = xpath.compile("/measCollecFile/measData/measInfo");
			xpathExpTableRow0 = xpath.compile("measValue[1]");
			xpathExpTableObjDesc = xpath.compile("./@measObjLdn");
			xpathExpTableAttrNames = xpath.compile("./measType");
			xpathExpTableRows = xpath.compile("./measValue");//values under table
			xpathExpTableRowValues = xpath.compile("./r");//values under row
			xpathExpTableSystemAttr1 = xpath.compile("./granPeriod/@endTime");
			xpathExpTableSystemAttr2 = xpath.compile("./granPeriod/@duration");
		}catch(Exception e){
			logger.info("", e);
		}
		
		this.xmlFolder = pc.getString(cfgkey_xml_folder);
		this.csvFolder = pc.getString(cfgkey_csv_folder);
		this.schemaHistoryFolder = pc.getString(cfgkey_schema_history_folder);
		this.dataHistoryFolder = pc.getString(cfgkey_data_history_folder);
		this.prefix = pc.getString(cfgkey_prefix);
		this.schemaFileName = pc.getString(cfgkey_schema_folder) + prefix +"." + schema_name;
		this.logicSchema = new LogicSchema();
		if (new File(schemaFileName).exists()){
			logicSchema = LogicSchema.fromFile(schemaFileName);
		}	
	}
	
	public static Document getDocument(String inputXml){
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document doc = builder.parse(inputXml);
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
	private void genData(Node mi, Map<String, String> localDnMap, List<String> orgSchemaAttributes, List<String> mtcl, String tableName, 
			Map<String, BufferedWriter> fvWriterMap, String startProcessTime) throws Exception {
		String sysValue1 = (String) xpathExpTableSystemAttr1.evaluate(mi, XPathConstants.STRING);
		String sysValue2 = (String) xpathExpTableSystemAttr2.evaluate(mi, XPathConstants.STRING);
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
			Node mv = mvl.item(k);
			mv.getParentNode().removeChild(mv);//for performance
			List<String> fieldValues = new ArrayList<String>();
			//system values
			fieldValues.add(sysValue1);
			fieldValues.add(sysValue2);
			fieldValues.add(localDnMap.get(SYSTEM_FIELD_SUBNETWORK));
			fieldValues.add(localDnMap.get(SYSTEM_FIELD_MANAGEDELEMENT));
			//object values
			String moldn = (String) xpathExpTableObjDesc.evaluate(mv, XPathConstants.STRING);
			TreeMap<String, String> kvs = Util.parseMapParams(moldn);
			for (String v:kvs.values()){
				fieldValues.add(v);
			}
			String[] vs = new String[orgSchemaAttributes.size()];
			NodeList rlist = (NodeList) xpathExpTableRowValues.evaluate(mv, XPathConstants.NODESET);
			for (int i=0; i<rlist.getLength(); i++){
				Node r = rlist.item(i);
				String v = r.getTextContent();
				vs[mapping.get(i)]=v;
			}
			fieldValues.addAll(Arrays.asList(vs));
			String outputFileName = getOutputDataFileName(tableName, startProcessTime);
			BufferedWriter osw = fvWriterMap.get(outputFileName);
			if (osw ==null) {
				osw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFileName)));
				fvWriterMap.put(outputFileName, osw);
			}
			String csv = Util.getCsv(fieldValues);
			osw.write(csv);
		}
	}
	
	private boolean checkSchemaUpdateOrGenData(String[] inputFileNames, String strProcessStartTime){
		boolean schemaUpdated=false;
		try {
			for (String inputFileName: inputFileNames){
				logger.debug(String.format("process %s", inputFileName));
				String ifn = xmlFolder + inputFileName;
				Document mf = XpathProcessor.getDocument(ifn);
				Map<String, String> localDnMap = Util.parseMapParams((String)xpathExpFileSystemAttr1.evaluate(mf, XPathConstants.STRING));
				NodeList ml = (NodeList) xpathExpTables.evaluate(mf, XPathConstants.NODESET);
				for (int i=0; i<ml.getLength(); i++){
					Node mi = ml.item(i);
					mi.getParentNode().removeChild(mi);//for performance
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
						tableAttrNamesList.add(mts.item(j).getTextContent());
					}
					if (orgSchemaAttributes!=null){
						//check new attribute
						List<String> newAttrNames = new ArrayList<String>();
						List<String> newAttrTypes = new ArrayList<String>();
						for (int j=0; j<tableAttrNamesList.size(); j++){
							String mtc = tableAttrNamesList.get(j);
							if (!orgSchemaAttributes.contains(mtc)){
								newAttrNames.add(mtc);
								newAttrTypes.add(Util.guessType(mv0vs.item(j).getTextContent()));
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
							onlyAttrTypesList.add(Util.guessType(mv0vs.item(j).getTextContent()));
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
	
	public void process(){
		String strProcessStartTime = sdf.format(new Date());
		File xmlFolderFile = new File(xmlFolder);
		String[] inputFileNames = xmlFolderFile.list();
		try {
			boolean schemaUpdated = checkSchemaUpdateOrGenData(inputFileNames, strProcessStartTime);
			//generate schema files
			if (schemaUpdated){
				List<String> createTableSqls = new ArrayList<String>();
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
						fieldNameList.addAll(systemFieldNames);
						fieldTypeList.addAll(systemFieldTypes);
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
					fieldNameList.addAll(systemFieldNames);
					fieldNameList.addAll(logicSchema.getObjParams(tn));
					fieldNameList.addAll(logicSchema.getAttrNames(tn));
					String copySql = Util.genCopySql(fieldNameList, tn, csvFolder, prefix);//having %s as the placeholder for real file name
					copysqls.add(copySql);
				}
				//execute the schemas
				Util.executeSqls(createTableSqls, pc);
				
				//gen schema files to history dir for reference
				Util.writeFile(String.format("%s%s.%s_%s", schemaHistoryFolder, prefix, createtablesql_name, strProcessStartTime), createTableSqls);
				Util.writeFile(String.format("%s%s.%s_%s", schemaHistoryFolder, prefix, droptablesql_name, strProcessStartTime), dropTableSqls);
				Util.writeFile(String.format("%s%s.%s_%s", schemaHistoryFolder, prefix, trunctablesql_name, strProcessStartTime), truncTableSqls);
				Util.writeFile(String.format("%s%s.%s_%s", schemaHistoryFolder, prefix, copysql_name, strProcessStartTime), copysqls);
				
				//gen logic schema
				LogicSchema.toFile(schemaFileName, logicSchema);
				
				//gen data again using new schema
				fvWriterMap.clear();
				schemaAttrNameUpdates.clear();
				checkSchemaUpdateOrGenData(inputFileNames, strProcessStartTime);
				
				//mv the raw-xml files to lake
				//TODO
			}
			//copy the data to db
			List<String> copysqls = new ArrayList<String>();
			for (String tn: tablesUsed){
				List<String> fieldNameList = new ArrayList<String>();
				fieldNameList.addAll(systemFieldNames);
				fieldNameList.addAll(logicSchema.getObjParams(tn));
				fieldNameList.addAll(logicSchema.getAttrNames(tn));
				String copySql = Util.genCopySql(fieldNameList, tn, csvFolder, prefix);//having %s as the placeholder for real file name
				copySql = copySql.replace("%s", getOutputDataFileName(tn, strProcessStartTime));
				copysqls.add(copySql);
			}
			Util.executeSqls(copysqls, pc);
			
			//mv the csv files to lake
			//TODO
		}catch(Exception e){
			logger.error("", e);
		}
	}
}
