package etl.cmd;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import bdap.util.Util;
import etl.util.DBUtil;
import etl.util.FieldType;
import etl.util.ParamUtil;
import etl.util.ScriptEngineUtil;
import etl.util.VarType;
import scala.Tuple2;

public class XmlToCsvCmd extends SchemaETLCmd implements Serializable{
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(XmlToCsvCmd.class);
	
	//cfgkey
	public static final String cfgkey_xml_folder="xml-folder";//for schema only
	
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
	
	//used to generate table name
	private transient List<String> keyWithValue = null;
	private transient List<String> keySkip = null;
	private transient List<String> fileLvlSystemFieldNames = null;
	private transient List<FieldType> fileLvlSystemFieldTypes = null;
	private transient List<String> tableLvlSystemFieldNames = null;
	private transient List<FieldType> tableLvlSystemFieldTypes = null;
	
	//
	private transient XPathExpression FileLvlSystemAttrsXpath;//file level
	private transient XPathExpression xpathExpTables;
	private transient XPathExpression xpathExpTableRow0;
	private transient XPathExpression xpathExpTableObjDesc;
	private transient XPathExpression xpathExpTableAttrNames;
	private transient XPathExpression xpathExpTableRows;
	private transient XPathExpression[] xpathExpTableSystemAttrs;//table level
	private transient XPathExpression xpathExpTableRowValues;

	public XmlToCsvCmd(){
		super();
	}
	
	public XmlToCsvCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);

		fileLvlSystemFieldNames = new ArrayList<String>();
		fileLvlSystemFieldTypes = new ArrayList<FieldType>();
		tableLvlSystemFieldNames = new ArrayList<String>();
		tableLvlSystemFieldTypes = new ArrayList<FieldType>();
		keyWithValue = Arrays.asList(super.getCfgStringArray(cfgkey_TableObjDesc_useValues));
		keySkip = Arrays.asList(super.getCfgStringArray(cfgkey_TableObjDesc_skipKeys));
		
		//
		XPathFactory xPathfactory = XPathFactory.newInstance();
		XPath xpath = xPathfactory.newXPath();
		try {
			
			FileLvlSystemAttrsXpath = xpath.compile(super.getCfgString(cfgkey_FileLvelSystemAttrs_xpath, null));
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
			xpathExpTableRows = xpath.compile(super.getCfgString(cfgkey_xpath_TableRows, null));
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
			//for schema only
			String xmlFolderExp = super.getCfgString(cfgkey_xml_folder, null);
			if (xmlFolderExp!=null){
				this.xmlFolder = (String) ScriptEngineUtil.eval(xmlFolderExp, VarType.STRING, super.getSystemVariables());
			}
		}catch(Exception e){
			logger.info("", e);
		}
		
	}
	
	private Document getDocument(String inputXml){
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			InputSource input = new InputSource(new StringReader(inputXml));
			Document doc = builder.parse(input);
			return doc;
		}catch(Exception e){
			logger.error("", e);
			return null;
		}
	}
	
	private Document getDocument(Path inputXml){
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			InputSource input = new InputSource(new BufferedReader(new InputStreamReader(fs.open(inputXml))));
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
	
	private String nodeListToString(NodeList nl){
		StringBuffer sb = new StringBuffer();
		for (int i=0; i<nl.getLength(); i++){
			sb.append(nl.item(i).getTextContent()).append(",");
		}
		return sb.toString();
	}
	/*
	 * @param: orgAttrs: attrs defined in the schema
	 * @param: newAttrs: attrs found in xml
	 */
	private List<Tuple2<String, String>> genData(Node mi, Map<String, String> localDnMap, List<String> orgAttrs, 
			List<String> newAttrs, String tableName) {
		List<Tuple2<String, String>> retList  = new ArrayList<Tuple2<String, String>>();
		try {
			List<String> tableLvlSystemAttValues = new ArrayList<String>();
			for (XPathExpression exp:xpathExpTableSystemAttrs){
				tableLvlSystemAttValues.add((String) exp.evaluate(mi, XPathConstants.STRING));
			}
			//gen value idx mapping
			Map<Integer,Integer> mapping = new HashMap<Integer, Integer>();//new attr to old attr idx mapping
			for (int i=0; i<orgAttrs.size(); i++){
				String attr = orgAttrs.get(i);
				int idx = newAttrs.indexOf(attr);
				if (idx!=-1){
					mapping.put(idx, i);
				}
			}
			NodeList mvl = (NodeList) xpathExpTableRows.evaluate(mi, XPathConstants.NODESET);
			for (int k=0; k<mvl.getLength(); k++){
				Node mv = getNode(mvl, k);
				try{
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
					TreeMap<String, String> kvs = ParamUtil.parseMapParams(moldn);
					for (String v:kvs.values()){
						fieldValues.add(v);
					}
					String[] vs = new String[orgAttrs.size()];
					NodeList rlist = (NodeList) xpathExpTableRowValues.evaluate(mv, XPathConstants.NODESET);
					if (rlist.getLength()>mapping.size()){//the value has fields then the type, schema has not been updated in advance
						logger.error(String.format("value has more fields then type, schema has to be updated in advance. ldn:%s, type:%s, values:%s", 
								moldn, mapping, nodeListToString(rlist)));
						continue;
					}
					for (int i=0; i<rlist.getLength(); i++){
						Node r = getNode(rlist, i);
						String v = r.getTextContent();
						int idx = mapping.get(i);
						vs[idx]=v;
					}
					fieldValues.addAll(Arrays.asList(vs));
					String csv = Util.getCsv(fieldValues, false);
					logger.debug(String.format("%s,%s", tableName, csv));
					retList.add(new Tuple2<String, String>(tableName, csv));
				}catch(Exception e){
					logger.error(String.format("exception: mv:%s, orgAttr:%s, newAttr:%s, mapping:%s", 
							mv.getTextContent(), orgAttrs, newAttrs, mapping), e);
					return retList;
				}
			}
		}catch(Exception e){
			logger.error("", e);
		}
		
		return retList;
	}
	
	//tableName to csv
	public List<Tuple2<String, String>> flatMapToPair(String text){
		super.init();
		try {
			logger.info(String.format("process %s", text));
			Document mf = getDocument(text);
			Map<String, String> localDnMap = ParamUtil.parseMapParams((String)FileLvlSystemAttrsXpath.evaluate(mf, XPathConstants.STRING));
			NodeList ml = (NodeList) xpathExpTables.evaluate(mf, XPathConstants.NODESET);
			List<Tuple2<String, String>> retList  = new ArrayList<Tuple2<String, String>>();
			for (int i=0; i<ml.getLength(); i++){
				Node mi = getNode(ml, i);
				Node mv0 = (Node)xpathExpTableRow0.evaluate(mi, XPathConstants.NODE);
				String moldn = (String) xpathExpTableObjDesc.evaluate(mv0, XPathConstants.STRING);
				TreeMap<String, String> moldParams = ParamUtil.parseMapParams(moldn);
				String tableName = generateTableName(moldParams);
				List<String> orgSchemaAttributes = null;
				if (logicSchema.hasTable(tableName)){
					List<String> tableAttrNamesList = new ArrayList<String>();//table attr name list
					NodeList mts = (NodeList) xpathExpTableAttrNames.evaluate(mi, XPathConstants.NODESET);
					for (int j=0; j<mts.getLength(); j++){
						Node mt = getNode(mts, j);
						tableAttrNamesList.add(mt.getTextContent());
					}
					
					orgSchemaAttributes = new ArrayList<String>();
					List<String> allAttributes = logicSchema.getAttrNames(tableName);
					allAttributes.removeAll(fileLvlSystemFieldNames);
					allAttributes.removeAll(tableLvlSystemFieldNames);
					allAttributes.removeAll(moldParams.keySet());
					orgSchemaAttributes.addAll(allAttributes);
					
					retList.addAll(genData(mi, localDnMap, orgSchemaAttributes, tableAttrNamesList, tableName));
				}
			}
			logger.info(String.format("file:%s generates %d tuple2.", text, retList.size()));
			return retList;
		}catch(Exception e){
			logger.error("", e);
			return null;
		}
	}
	
	@Override
	public JavaPairRDD<String, String> sparkVtoKvProcess(JavaRDD<String> input, JavaSparkContext jsc){
		JavaPairRDD<String, String> ret = input.flatMapToPair(new PairFlatMapFunction<String, String, String>(){
			@Override
			public Iterator<Tuple2<String, String>> call(String t) throws Exception {
				return flatMapToPair(t).iterator();
			}
		});
		return ret;
	}
	
	/**
	 * @param row: each row is a xml file name
	 */
	@Override
	public Map<String, Object> mapProcess(long offset, String text, Mapper<LongWritable, Text, Text, Text>.Context context){
		try {
			List<Tuple2<String, String>> pairs = flatMapToPair(text);
			for (Tuple2<String, String> pair: pairs){
				context.write(new Text(pair._1), new Text(pair._2));
			}
		}catch(Exception e){
			logger.error("", e);
		}
		return null;
	}
	
	/**
	 * @return newKey, newValue, baseOutputPath
	 */
	@Override
	public List<String[]> reduceProcess(Text key, Iterable<Text> values, 
			Reducer<Text, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos) throws Exception{
		List<String[]> rets = new ArrayList<String[]>();
		for (Text v: values){
			//logger.info(String.format("v:%s", v.toString()));
			String[] ret = new String[]{v.toString(), null, key.toString()};
			rets.add(ret);
		}
		return rets;
	}
	
	//
	private String xmlFolder;
	private Map<String, List<String>> schemaAttrNameUpdates = new HashMap<String, List<String>>();//store updated/new tables' attribute parts' name (compared with the org schema)
	private Map<String, List<FieldType>> schemaAttrTypeUpdates = new HashMap<String, List<FieldType>>();//store updated/new tables' attribute parts' type (compared with the org schema)
	private Map<String, List<String>> newTableObjNamesAdded = new HashMap<String, List<String>>();//store new tables' obj name
	private Map<String, List<FieldType>> newTableObjTypesAdded = new HashMap<String, List<FieldType>>();//store new tables' obj type
	private Set<String> tablesUsed = new HashSet<String>(); //the tables this batch of data used
	
	private boolean checkSchemaUpdate(FileStatus[] inputFileNames){
		boolean schemaUpdated=false;
		try {
			for (FileStatus inputFile: inputFileNames){
				logger.debug(String.format("process %s", inputFile));
				Document mf = getDocument(inputFile.getPath());
				NodeList ml = (NodeList) xpathExpTables.evaluate(mf, XPathConstants.NODESET);
				for (int i=0; i<ml.getLength(); i++){
					Node mi = getNode(ml, i);
					Node mv0 = (Node)xpathExpTableRow0.evaluate(mi, XPathConstants.NODE);
					String moldn = (String) xpathExpTableObjDesc.evaluate(mv0, XPathConstants.STRING);
					TreeMap<String, String> moldParams = ParamUtil.parseMapParams(moldn);
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
