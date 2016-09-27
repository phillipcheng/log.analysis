package etl.cmd;

import java.io.BufferedReader;
import java.io.InputStreamReader;
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

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import etl.util.DBUtil;
import etl.util.FieldType;
import etl.util.ScriptEngineUtil;
import etl.util.Util;
import etl.util.VarType;

public class SchemaFromXmlCmd extends SchemaFileETLCmd{
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(SchemaFromXmlCmd.class);
	
	public static final String cfgkey_xml_folder="xml-folder";
	
	public static final String cfgkey_FileLvelSystemAttrs_xpath="FileSystemAttrs.xpath";
	public static final String cfgkey_FileLvelSystemAttrs_name="FileSystemAttrs.name";
	public static final String cfgkey_FileLvelSystemAttrs_type="FileSystemAttrs.type";

	public static final String cfgkey_TableObjDesc_xpath="TableObjDesc.xpath";
	public static final String cfgkey_TableObjDesc_skipKeys="TableObjDesc.skipKeys";
	public static final String cfgkey_TableObjDesc_useValues="TableObjDesc.useValues";
	
	public static final String cfgkey_xpath_Tables="xpath.Tables";
	public static final String cfgkey_xpath_TableRow0="xpath.TableRow0";
	public static final String cfgkey_xpath_TableAttrNames="xpath.TableAttrNames";
	public static final String cfgkey_xpath_TableRows="xpath.TableRows";
	public static final String cfgkey_xpath_TableRowValues="xpath.TableRowValues";
	
	public static final String cfgkey_TableSystemAttrs_xpath="TableSystemAttrs.xpath";
	public static final String cfgkey_TableSystemAttrs_name="TableSystemAttrs.name";
	public static final String cfgkey_TableSystemAttrs_type="TableSystemAttrs.type";
	
	public static final String createtablesql_name="createtables.sql";
	
	//used to generate table name
	private List<String> keyWithValue = new ArrayList<String>();
	private List<String> keySkip = new ArrayList<String>();
	private List<String> fileLvlSystemFieldNames = new ArrayList<String>();
	private List<FieldType> fileLvlSystemFieldTypes = new ArrayList<FieldType>();
	private List<String> tableLvlSystemFieldNames = new ArrayList<String>();
	private List<FieldType> tableLvlSystemFieldTypes = new ArrayList<FieldType>();
	
	private String xmlFolder;
	private XPathExpression xpathExpTables;
	private XPathExpression xpathExpTableRow0;
	private XPathExpression xpathExpTableObjDesc;
	private XPathExpression xpathExpTableAttrNames;
	private XPathExpression[] xpathExpTableSystemAttrs;//table level
	private XPathExpression xpathExpTableRowValues;
	//
	private Map<String, List<String>> schemaAttrNameUpdates = new HashMap<String, List<String>>();//store updated/new tables' attribute parts' name (compared with the org schema)
	private Map<String, List<FieldType>> schemaAttrTypeUpdates = new HashMap<String, List<FieldType>>();//store updated/new tables' attribute parts' type (compared with the org schema)
	private Map<String, List<String>> newTableObjNamesAdded = new HashMap<String, List<String>>();//store new tables' obj name
	private Map<String, List<FieldType>> newTableObjTypesAdded = new HashMap<String, List<FieldType>>();//store new tables' obj type
	private Set<String> tablesUsed = new HashSet<String>(); //the tables this batch of data used
	
	
	public SchemaFromXmlCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
		keyWithValue = Arrays.asList(super.getCfgStringArray(cfgkey_TableObjDesc_useValues));
		keySkip = Arrays.asList(super.getCfgStringArray(cfgkey_TableObjDesc_skipKeys));
		//
		XPathFactory xPathfactory = XPathFactory.newInstance();
		XPath xpath = xPathfactory.newXPath();
		try {
			xpath.compile(super.getCfgString(cfgkey_FileLvelSystemAttrs_xpath, null));
			for (String name: super.getCfgStringArray(cfgkey_FileLvelSystemAttrs_name)){
				fileLvlSystemFieldNames.add(name);
			}
			for (String type: super.getCfgStringArray(cfgkey_FileLvelSystemAttrs_type)){
				fileLvlSystemFieldTypes.add(new FieldType(type));
			}
			xpathExpTables = xpath.compile(super.getCfgString(cfgkey_xpath_Tables, null));
			xpathExpTableRow0 = xpath.compile(super.getCfgString(cfgkey_xpath_TableRow0, null));
			xpathExpTableObjDesc = xpath.compile(super.getCfgString(cfgkey_TableObjDesc_xpath, null));
			xpathExpTableAttrNames = xpath.compile(super.getCfgString(cfgkey_xpath_TableAttrNames, null));
			xpath.compile(super.getCfgString(cfgkey_xpath_TableRows, null));
			xpathExpTableRowValues = xpath.compile(super.getCfgString(cfgkey_xpath_TableRowValues, null));
			String[] tsxpaths = super.getCfgStringArray(cfgkey_TableSystemAttrs_xpath);
			xpathExpTableSystemAttrs = new XPathExpression[tsxpaths.length];
			for (int i=0; i<tsxpaths.length; i++){
				xpathExpTableSystemAttrs[i]=xpath.compile(tsxpaths[i]);
			}
			for (String name: super.getCfgStringArray(cfgkey_TableSystemAttrs_name)){
				tableLvlSystemFieldNames.add(name);
			}
			for (String type: super.getCfgStringArray(cfgkey_TableSystemAttrs_type)){
				tableLvlSystemFieldTypes.add(new FieldType(type));
			}
			String xmlFolderExp = super.getCfgString(cfgkey_xml_folder, null);
			if (xmlFolderExp==null){
				logger.error(String.format("xml folder can't be null."));
				return;
			}
			this.xmlFolder = (String) ScriptEngineUtil.eval(xmlFolderExp, VarType.STRING, super.getSystemVariables());
		}catch(Exception e){
			logger.info("", e);
		}
	}
	
	private Document getDocument(FileStatus inputXml){
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
	
	private String generateTableName(TreeMap<String, String> moldParams){
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
	
	private Node getNode(NodeList nl, int idx){
		Node n = nl.item(idx);
		n.getParentNode().removeChild(n);//for performance
		return n;
	}
	
	/**
	 */
	private boolean checkSchemaUpdate(FileStatus[] inputFileNames){
		boolean schemaUpdated=false;
		try {
			for (FileStatus inputFile: inputFileNames){
				logger.debug(String.format("process %s", inputFile));
				Document mf = getDocument(inputFile);
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
					if (logicSchema.hasTable(tableName)){
						orgSchemaAttributes = new ArrayList<String>();
						List<String> allAttributes = logicSchema.getAttrNames(tableName);
						allAttributes.removeAll(fileLvlSystemFieldNames);
						allAttributes.removeAll(tableLvlSystemFieldNames);
						allAttributes.removeAll(moldParams.keySet());
						orgSchemaAttributes.addAll(allAttributes);
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
						List<FieldType> newAttrTypes = new ArrayList<FieldType>();
						for (int j=0; j<tableAttrNamesList.size(); j++){
							String mtc = tableAttrNamesList.get(j);
							if (!orgSchemaAttributes.contains(mtc)){
								newAttrNames.add(mtc);
								Node mv0vj = getNode(mv0vs, j);
								newAttrTypes.add(DBUtil.guessDBType(mv0vj.getTextContent()));
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
						}
					}else{
						//new table needed
						List<FieldType> onlyAttrTypesList = new ArrayList<FieldType>();
						for (int j=0; j<mv0vs.getLength(); j++){
							Node mv0vj = getNode(mv0vs, j);
							onlyAttrTypesList.add(DBUtil.guessDBType(mv0vj.getTextContent()));
						}
						schemaAttrNameUpdates.put(tableName, tableAttrNamesList);
						schemaAttrTypeUpdates.put(tableName, onlyAttrTypesList);
						//
						List<String> objNameList = new ArrayList<String>();
						List<FieldType> objTypeList = new ArrayList<FieldType>();
						for (String key: moldParams.keySet()){
							objNameList.add(key);
							objTypeList.add(DBUtil.guessDBType(moldParams.get(key)));
						}
						newTableObjNamesAdded.put(tableName, objNameList);
						newTableObjTypesAdded.put(tableName, objTypeList);
						schemaUpdated=true;
					}
				}
			}
		}catch(Exception e){
			logger.error("", e);
		}
		return schemaUpdated;
	}
	
	/*
	 * output: schema, createsql
	*/
	@Override
	public List<String> sgProcess() {
		List<String> logInfo = new ArrayList<String>();
		try {
			FileStatus[] inputFileNames = fs.listStatus(new Path(xmlFolder));
			logInfo.add(inputFileNames.length + "");//number of input files
			boolean schemaUpdated = checkSchemaUpdate(inputFileNames);
			if (schemaUpdated){//updated
				List<String> createTableSqls = new ArrayList<String>();
				List<String> sysAttrNames = new ArrayList<String>();
				//set sys attr names into logic schema
				sysAttrNames.addAll(tableLvlSystemFieldNames);
				sysAttrNames.addAll(fileLvlSystemFieldNames);
				//
				for(String tn:schemaAttrNameUpdates.keySet()){
					List<String> fieldNameList = new ArrayList<String>(); 
					List<FieldType> fieldTypeList = new ArrayList<FieldType>();
					if (logicSchema.getAttrNames(tn)!=null){//update table
						fieldNameList.addAll(schemaAttrNameUpdates.get(tn));
						fieldTypeList.addAll(schemaAttrTypeUpdates.get(tn));
						//update schema
						logicSchema.addAttributes(tn, schemaAttrNameUpdates.get(tn));
						logicSchema.addAttrTypes(tn, schemaAttrTypeUpdates.get(tn));
						//gen update sql
						createTableSqls.addAll(DBUtil.genUpdateTableSql(fieldNameList, fieldTypeList, tn, 
								dbPrefix, super.getDbtype()));
					}else{//new table
						//gen create sql
						fieldNameList.addAll(tableLvlSystemFieldNames);
						fieldNameList.addAll(fileLvlSystemFieldNames);
						fieldTypeList.addAll(tableLvlSystemFieldTypes);
						fieldTypeList.addAll(fileLvlSystemFieldTypes);
						fieldNameList.addAll(newTableObjNamesAdded.get(tn));
						fieldTypeList.addAll(newTableObjTypesAdded.get(tn));
						fieldNameList.addAll(schemaAttrNameUpdates.get(tn));
						fieldTypeList.addAll(schemaAttrTypeUpdates.get(tn));
						//update to logic schema
						logicSchema.updateTableAttrs(tn, fieldNameList);
						logicSchema.updateTableAttrTypes(tn, fieldTypeList);
						//
						createTableSqls.add(DBUtil.genCreateTableSql(fieldNameList, fieldTypeList, tn, 
								dbPrefix, super.getDbtype()));
					}
					//gen copys.sql for reference
					List<String> attrs = new ArrayList<String>();
					attrs.addAll(logicSchema.getAttrNames(tn));
				}
				logInfo.addAll(super.updateDynSchema(createTableSqls));
			}
		}catch(Exception e){
			logger.error("", e);
		}
		return logInfo;
	}
}
