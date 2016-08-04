package etl.cmd.dynschema;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import etl.engine.DynaSchemaFileETLCmd;
import etl.util.Util;

public class XmlToCsvCmd extends DynaSchemaFileETLCmd{
	public static final Logger logger = Logger.getLogger(XmlToCsvCmd.class);
	
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
	private List<String> keyWithValue = new ArrayList<String>();
	private List<String> keySkip = new ArrayList<String>();
	private List<String> fileLvlSystemFieldNames = new ArrayList<String>();
	private List<String> fileLvlSystemFieldTypes = new ArrayList<String>();
	private List<String> tableLvlSystemFieldNames = new ArrayList<String>();
	private List<String> tableLvlSystemFieldTypes = new ArrayList<String>();
	
	private String xmlFolder;
	//
	private XPathExpression FileLvlSystemAttrsXpath;//file level
	private XPathExpression xpathExpTables;
	private XPathExpression xpathExpTableRow0;
	private XPathExpression xpathExpTableObjDesc;
	private XPathExpression xpathExpTableAttrNames;
	private XPathExpression xpathExpTableRows;
	private XPathExpression[] xpathExpTableSystemAttrs;//table level
	private XPathExpression xpathExpTableRowValues;

	public XmlToCsvCmd(String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		super(wfid, staticCfg, defaultFs, otherArgs);
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
			this.xmlFolder = pc.getString(cfgkey_xml_folder);
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
	private void genData(Node mi, Map<String, String> localDnMap, List<String> orgAttrs, List<String> newAttrs, String tableName, 
			Mapper<LongWritable, Text, Text, Text>.Context context) throws Exception {
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
			context.write(new Text(tableName), new Text(csv));
		}
	}
	
	/**
	 * @param row: each row is a xml file name
	 */
	@Override
	public Map<String, String> reduceMapProcess(long offset, String row, Mapper<LongWritable, Text, Text, Text>.Context context){
		try {
			logger.debug(String.format("process %s", row));
			Document mf = getDocument(new Path(row));
			Map<String, String> localDnMap = Util.parseMapParams((String)FileLvlSystemAttrsXpath.evaluate(mf, XPathConstants.STRING));
			NodeList ml = (NodeList) xpathExpTables.evaluate(mf, XPathConstants.NODESET);
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
					
					genData(mi, localDnMap, orgSchemaAttributes, tableAttrNamesList, tableName, context);
				}
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
			logger.info(String.format("v:%s", v.toString()));
			String[] ret = new String[]{v.toString(), "", key.toString()};
			rets.add(ret);
		}
		return rets;
	}
}
