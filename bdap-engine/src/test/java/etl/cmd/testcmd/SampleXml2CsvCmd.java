package etl.cmd.testcmd;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import bdap.util.ParamUtil;
import bdap.util.XmlUtil;
import etl.cmd.dynschema.DynamicSchemaCmd;
import etl.cmd.dynschema.DynamicTableSchema;
import etl.engine.LogicSchema;
import etl.engine.types.ProcessMode;
import etl.util.FieldType;
import etl.util.VarType;

public class SampleXml2CsvCmd extends DynamicSchemaCmd{
	
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(SampleXml2CsvCmd.class);
	
	private transient List<String> keyWithValue = null;
	private transient List<String> keySkip = null;
	private transient List<String> fileLvlSystemFieldNames = null;
	private transient List<FieldType> fileLvlSystemFieldTypes = null;
	private transient List<String> tableLvlSystemFieldNames = null;
	private transient List<FieldType> tableLvlSystemFieldTypes = null;
	//
	private transient XPathExpression fileLvlSystemAttrsXpath;//file level
	private transient XPathExpression xpathExpTable;
	private transient XPathExpression xpathExpTableRow0;
	private transient XPathExpression xpathExpTableObjDesc;
	private transient XPathExpression xpathExpTableAttrNames;
	private transient XPathExpression xpathExpTableRows;
	private transient XPathExpression[] xpathExpTableSystemAttrs;//table level
	private transient XPathExpression xpathExpTableRowValues;
	
	private transient Document doc;
	private transient Node table;
	private transient List<String> tableLvlSystemAttValues = null;
	private transient Map<String, String> localDnMap = null;

	public SampleXml2CsvCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	public SampleXml2CsvCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs, ProcessMode pm){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, pm);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs, ProcessMode pm){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
		keyWithValue = Arrays.asList(new String[]{"PoolType"});
		keySkip = Arrays.asList(new String[]{"Machine","UUID","PoolId","PoolMember"});
		fileLvlSystemFieldNames = Arrays.asList(new String[]{"SubNetwork", "ManagedElement"});
		fileLvlSystemFieldTypes = Arrays.asList(new FieldType[]{new FieldType(VarType.STRING, 70), new FieldType(VarType.STRING, 70)});
		tableLvlSystemFieldNames = Arrays.asList(new String[]{"endTime", "duration"});
		tableLvlSystemFieldTypes = Arrays.asList(new FieldType[]{new FieldType(VarType.TIMESTAMP), new FieldType(VarType.STRING, 10)});
		
		XPathFactory xPathfactory = XPathFactory.newInstance();
		XPath xpath = xPathfactory.newXPath();
		tableLvlSystemAttValues = new ArrayList<String>();
		try {
			fileLvlSystemAttrsXpath = xpath.compile("/measCollecFile/fileHeader/fileSender/@localDn");
			xpathExpTableSystemAttrs = new XPathExpression[]{xpath.compile("./granPeriod/@endTime"), xpath.compile("./granPeriod/@duration")};
			xpathExpTable = xpath.compile("/measCollecFile/measData/measInfo[1]");
			xpathExpTableRow0 = xpath.compile("measValue[1]");
			xpathExpTableObjDesc = xpath.compile("./@measObjLdn");
			xpathExpTableAttrNames = xpath.compile("./measType");
			xpathExpTableRows = xpath.compile("./measValue");
			xpathExpTableRowValues = xpath.compile("./r");
		}catch(Exception e){
			logger.error("", e);
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
	//contains all attributes
	private String[] getValuesFromRow(Node mv){
		try{
			List<String> fieldValues = new ArrayList<String>();
			//system values
			for (String v:tableLvlSystemAttValues){
				fieldValues.add(v);
			}
			for (String n:fileLvlSystemFieldNames){
				fieldValues.add(localDnMap.get(n));	
			}
			//dimension values
			String moldn = (String) xpathExpTableObjDesc.evaluate(mv, XPathConstants.STRING);
			TreeMap<String, String> kvs = ParamUtil.parseMapParams(moldn);
			for (String v:kvs.values()){
				fieldValues.add(v);
			}
			NodeList rlist = (NodeList) xpathExpTableRowValues.evaluate(mv, XPathConstants.NODESET);
			for (int i=0; i<rlist.getLength(); i++){
				Node r = XmlUtil.getNode(rlist, i);
				String v = r.getTextContent();
				fieldValues.add(v);
			}
			String[] vs = fieldValues.toArray(new String[]{});
			return vs;
		}catch(Exception e){
			logger.error(String.format("exception: mv:%s", mv.getTextContent()), e);
			return null;
		}
	}
	
	@Override
	public DynamicTableSchema getDynamicTable(String input, LogicSchema ls) throws Exception{
		doc = XmlUtil.getDocument(input);
		localDnMap = ParamUtil.parseMapParams((String)fileLvlSystemAttrsXpath.evaluate(doc, XPathConstants.STRING));
		table = (Node) xpathExpTable.evaluate(doc, XPathConstants.NODE);
		//table system attr values
		tableLvlSystemAttValues.clear();
		for (XPathExpression exp:xpathExpTableSystemAttrs){
			tableLvlSystemAttValues.add((String) exp.evaluate(table, XPathConstants.STRING));
		}
		Node mv0 = (Node)xpathExpTableRow0.evaluate(table, XPathConstants.NODE);
		String moldn = (String) xpathExpTableObjDesc.evaluate(mv0, XPathConstants.STRING);
		TreeMap<String, String> moldParams = ParamUtil.parseMapParams(moldn);
		//tableName
		String tableName = generateTableName(moldParams);
		//attrNames
		List<String> tableAttrNamesList = new ArrayList<String>();//table attr name list
		//variable attr
		NodeList mts = (NodeList) xpathExpTableAttrNames.evaluate(table, XPathConstants.NODESET);
		for (int j=0; j<mts.getLength(); j++){
			Node mt = XmlUtil.getNode(mts, j);
			tableAttrNamesList.add(mt.getTextContent());
		}
		tableAttrNamesList.addAll(0, moldParams.keySet());
		//fixed attr
		tableAttrNamesList.addAll(0, fileLvlSystemFieldNames);
		tableAttrNamesList.addAll(0, tableLvlSystemFieldNames);
		//get sample measure values
		String[] vs = getValuesFromRow(mv0);
		List<FieldType> types = new ArrayList<FieldType>();
		types.addAll(fileLvlSystemFieldTypes);
		types.addAll(tableLvlSystemFieldTypes);

		doc = XmlUtil.getDocument(input);
		table = (Node) xpathExpTable.evaluate(doc, XPathConstants.NODE);

		return new DynamicTableSchema(tableName, tableAttrNamesList, vs, types);
	}

	@Override
	public List<String[]> getValues() throws Exception {
		//values
		List<String[]> rowValues = new ArrayList<String[]>();
		NodeList mvl = (NodeList) xpathExpTableRows.evaluate(table, XPathConstants.NODESET);
		for (int k=0; k<mvl.getLength(); k++){
			Node mv = XmlUtil.getNode(mvl, k);
			String[] vs = getValuesFromRow(mv);
			if (vs!=null){
				rowValues.add(vs);
			}
		}
		return rowValues;
	}
}

