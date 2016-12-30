package etl.flow.test.cmd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import bdap.util.XmlUtil;
import etl.cmd.dynschema.DynamicSchemaCmd;
import etl.cmd.dynschema.DynamicTableSchema;
import etl.engine.LogicSchema;
import etl.engine.ProcessMode;
import etl.util.FieldType;
import etl.util.VarType;

public class Flow1Xml2CsvCmd extends DynamicSchemaCmd{
	
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(Flow1Xml2CsvCmd.class);
	
	private List<String> names = null;
	private List<FieldType> types = null;
	private transient XPathExpression xpathExpTable;
	private transient XPathExpression xpathExpTableRows;
	private transient XPathExpression xpathExpTableRowValues;
	
	private transient Document doc;
	private transient Node table;

	public Flow1Xml2CsvCmd(){
	}
	
	public Flow1Xml2CsvCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	public Flow1Xml2CsvCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs, ProcessMode pm){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, pm);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs, ProcessMode pm){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
		
		XPathFactory xPathfactory = XPathFactory.newInstance();
		XPath xpath = xPathfactory.newXPath();
		try {
			types =  Arrays.asList(new FieldType[]{new FieldType(VarType.STRING, 20), 
					new FieldType(VarType.STRING, 20), 
					new FieldType(VarType.STRING, 20), 
					new FieldType(VarType.NUMERIC, 10, 2),
					new FieldType(VarType.NUMERIC, 10, 2)
					});
			names = Arrays.asList(new String[]{"sid", "a2id", "p2id", "p2v1", "p2v2"});
			xpathExpTable = xpath.compile("/data/info");
			xpathExpTableRows = xpath.compile("./values");
			xpathExpTableRowValues = xpath.compile("./text()");
		}catch(Exception e){
			logger.error("", e);
		}
	}
	
	@Override
	public DynamicTableSchema getDynamicTable(String input, LogicSchema ls) throws Exception{
		doc = XmlUtil.getDocument(input);
		table = (Node) xpathExpTable.evaluate(doc, XPathConstants.NODE);
		String tableName = "data2";
		return new DynamicTableSchema(tableName, names, null, types);
	}

	@Override
	public List<String[]> getValues() throws Exception {
		//values
		List<String[]> rowValues = new ArrayList<String[]>();
		NodeList mvl = (NodeList) xpathExpTableRows.evaluate(table, XPathConstants.NODESET);
		for (int k=0; k<mvl.getLength(); k++){
			Node mv = XmlUtil.getNode(mvl, k);
			String rlist = (String) xpathExpTableRowValues.evaluate(mv, XPathConstants.STRING);
			String[] vs = rlist.split(",");
			if (vs!=null){
				rowValues.add(vs);
			}
		}
		return rowValues;
	}
}

