package etl.cmd;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import etl.util.Util;

import scala.Tuple2;

public class XmlToCsvCmd extends SchemaFileETLCmd implements Serializable{
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(XmlToCsvCmd.class);
	
	public static final String cfgkey_xml_folder="xml-folder";
	public static final String cfgkey_csv_folder="csv-folder";
	public static final String cfgkey_schema_file="schema.file";
	
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
	private transient List<String> keyWithValue = new ArrayList<String>();
	private transient List<String> keySkip = new ArrayList<String>();
	private transient List<String> fileLvlSystemFieldNames = new ArrayList<String>();
	private transient List<String> fileLvlSystemFieldTypes = new ArrayList<String>();
	private transient List<String> tableLvlSystemFieldNames = new ArrayList<String>();
	private transient List<String> tableLvlSystemFieldTypes = new ArrayList<String>();
	
	//
	private transient XPathExpression FileLvlSystemAttrsXpath;//file level
	private transient XPathExpression xpathExpTables;
	private transient XPathExpression xpathExpTableRow0;
	private transient XPathExpression xpathExpTableObjDesc;
	private transient XPathExpression xpathExpTableAttrNames;
	private transient XPathExpression xpathExpTableRows;
	private transient XPathExpression[] xpathExpTableSystemAttrs;//table level
	private transient XPathExpression xpathExpTableRowValues;

	public XmlToCsvCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, defaultFs, otherArgs);
	}
	
	public void init(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		super.init(wfName, wfid, staticCfg, defaultFs, otherArgs);
		keyWithValue = new ArrayList<String>();
		keySkip = new ArrayList<String>();
		fileLvlSystemFieldNames = new ArrayList<String>();
		fileLvlSystemFieldTypes = new ArrayList<String>();
		tableLvlSystemFieldNames = new ArrayList<String>();
		tableLvlSystemFieldTypes = new ArrayList<String>();
		
		keyWithValue = Arrays.asList(pc.getStringArray(cfgkey_TableObjDesc_useValues));
		keySkip = Arrays.asList(pc.getStringArray(cfgkey_TableObjDesc_skipKeys));
		
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
			xpathExpTableObjDesc = xpath.compile(pc.getString(cfgkey_TableObjDesc_xpath));
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
		}catch(Exception e){
			logger.info("", e);
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
	
	/*
	 * @param: orgAttrs: attrs defined in the schema
	 * @param: newAttrs: attrs found in xml
	 */
	private List<Tuple2<String, String>> genData(Node mi, Map<String, String> localDnMap, List<String> orgAttrs, 
			List<String> newAttrs, String tableName) throws Exception {
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
		List<Tuple2<String, String>> retList  = new ArrayList<Tuple2<String, String>>();
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
			String[] vs = new String[orgAttrs.size()];
			NodeList rlist = (NodeList) xpathExpTableRowValues.evaluate(mv, XPathConstants.NODESET);
			for (int i=0; i<rlist.getLength(); i++){
				Node r = getNode(rlist, i);
				String v = r.getTextContent();
				vs[mapping.get(i)]=v;
			}
			fieldValues.addAll(Arrays.asList(vs));
			String csv = Util.getCsv(fieldValues, false);
			logger.debug(String.format("%s,%s", tableName, csv));
			retList.add(new Tuple2<String, String>(tableName, csv));
		}
		
		return retList;
	}
	
	//tableName to csv
	public List<Tuple2<String, String>> flatMapToPair(String key, String value){
		super.init();
		try {
			String row = value.toString();
			//logger.info(String.format("process %s", row));
			Document mf = getDocument(new Path(row));
			Map<String, String> localDnMap = Util.parseMapParams((String)FileLvlSystemAttrsXpath.evaluate(mf, XPathConstants.STRING));
			NodeList ml = (NodeList) xpathExpTables.evaluate(mf, XPathConstants.NODESET);
			List<Tuple2<String, String>> retList  = new ArrayList<Tuple2<String, String>>();
			for (int i=0; i<ml.getLength(); i++){
				Node mi = getNode(ml, i);
				Node mv0 = (Node)xpathExpTableRow0.evaluate(mi, XPathConstants.NODE);
				String moldn = (String) xpathExpTableObjDesc.evaluate(mv0, XPathConstants.STRING);
				TreeMap<String, String> moldParams = Util.parseMapParams(moldn);
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
			logger.info(String.format("file:%s generates %d tuple2.", row, retList.size()));
			return retList;
		}catch(Exception e){
			logger.error("", e);
			return null;
		}
	}
	
	@Override
	public JavaRDD<Tuple2<String, String>> sparkProcess(JavaRDD<Tuple2<String, String>> input, JavaSparkContext jsc){
		JavaRDD<Tuple2<String, String>> ret = input.flatMap(new FlatMapFunction<Tuple2<String, String>, 
				Tuple2<String, String>>(){
			private static final long serialVersionUID = 1L;
			@Override
			public Iterator<Tuple2<String, String>> call(Tuple2<String, String> t) throws Exception {
				return flatMapToPair(t._1, t._2).iterator();
			}
		});
		return ret;
	}
	/**
	 * @param row: each row is a xml file name
	 */
	@Override
	public Map<String, Object> mapProcess(long offset, String row, Mapper<LongWritable, Text, Text, Text>.Context context){
		try {
			List<Tuple2<String, String>> pairs = flatMapToPair(String.valueOf(offset), row);
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
	public List<String[]> reduceProcess(Text key, Iterable<Text> values){
		List<String[]> rets = new ArrayList<String[]>();
		for (Text v: values){
			//logger.info(String.format("v:%s", v.toString()));
			String[] ret = new String[]{v.toString(), null, key.toString()};
			rets.add(ret);
		}
		return rets;
	}
}
