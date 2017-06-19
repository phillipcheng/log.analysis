package etl.cmd.dynschema;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.Node;

import bdap.util.Util;
import etl.cmd.SchemaETLCmd;
import etl.engine.ETLCmd;
import etl.engine.EngineUtil;
import etl.engine.types.OutputType;
import etl.engine.types.ProcessMode;
import etl.util.ConfigKey;
import etl.util.FieldType;
import etl.util.GroupFun;
import etl.util.ScriptEngineUtil;
import etl.util.XmlTableProperties;
import scala.Tuple2;
import scala.Tuple3;

public class Xml2CsvCmd extends SchemaETLCmd{
	private static final long serialVersionUID = 1L;
	
	public static final Logger logger = LogManager.getLogger(Xml2CsvCmd.class);
	
	public static final @ConfigKey String cfgkey_node_root_path="node.root.path";
	public static final @ConfigKey String cfgkey_tables="tables";
	public static final @ConfigKey String cfgkey_node_start=".node.start";
	public static final @ConfigKey String cfgkey_node_leafs=".node.leafs";
	public static final @ConfigKey String cfgkey_node_skip=".node.skip";
	public static final @ConfigKey String cfgkey_node_associate=".node.associate";
	public static final @ConfigKey String cfgkey_table_common_element="common.node.associate";
	
	public static final @ConfigKey String cfgkey_table_field_stringformat=".field.string.format";
	
	//special for pde_4G
	public static final @ConfigKey String cfgkey_table_field_use_filename=".field.use.filename";
	public static final @ConfigKey(type=Boolean.class) String cfgkey_table_trans_requirment_ttff=".requirment.TTFF";
	
	private String nodeRootPath;
	private String[] tables;
	private transient Map<String, XmlTableProperties> tablePropertiesMap;
	
	
	
	
	public Xml2CsvCmd(){
	}
	
	public Xml2CsvCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	public Xml2CsvCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs, ProcessMode pm){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, pm);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs, ProcessMode pm){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
		nodeRootPath = super.getCfgString(cfgkey_node_root_path,null);
		tables = super.getCfgStringArray(cfgkey_tables);
		initTablePropertiesMap();
		
	}
	
	private void initTablePropertiesMap(){
		tablePropertiesMap = new HashMap<String, XmlTableProperties>();
		String[] commonElement = super.getCfgStringArray(cfgkey_table_common_element);
		for(String table:tables){
			String startNode = super.getCfgString(table + cfgkey_node_start , null);
			String[] stopNode = super.getCfgStringArray(table + cfgkey_node_leafs);
			String[] parentNode = super.getCfgStringArray(table + cfgkey_node_associate);
			String[] skipNode = super.getCfgStringArray(table + cfgkey_node_skip);
			List<String > parentNodeList = new ArrayList<String>();
			parentNodeList.addAll(Arrays.asList(commonElement));
			parentNodeList.addAll(Arrays.asList(parentNode));
			XmlTableProperties tableProperty = new XmlTableProperties(table,startNode, Arrays.asList(stopNode), parentNodeList,Arrays.asList(skipNode));
			Map<String,String> formatTimeMap = initFormatTimeConfig(table);
			tableProperty.setFormatTimeMap(formatTimeMap);
			String useFileName = super.getCfgString(table + cfgkey_table_field_use_filename , null);
			tableProperty.setUseFileName(useFileName);
			boolean pde4G = super.getCfgBoolean(table + cfgkey_table_trans_requirment_ttff, false);
			tableProperty.setPde4G(pde4G);
			tablePropertiesMap.put(table, tableProperty);
		}
		
	}
	private Map<String,String> initFormatTimeConfig(String table){
		Map<String,String> strFormatMap = new HashMap<String,String>();
		String[] formatTimeArr = super.getCfgStringArray(table + cfgkey_table_field_stringformat);
		if(formatTimeArr == null){
			return strFormatMap;
		}
		for(String str:formatTimeArr){
			int index = str.indexOf(":");
			if(index != -1){
				strFormatMap.put(str.substring(0,index), str.substring(index+1));
			}
			
		}
		return strFormatMap;
	}
	private String formatString(String str,String formatStr){
		if(str == null){
			return null;
		}
		if(formatStr.startsWith("timeformat:")){
			return GroupFun.dtStandardize(str, formatStr.replace("timeformat:", ""));
		}else if(formatStr.startsWith("findStr:")){
			Pattern pattern = Pattern.compile(formatStr.replace("findStr:", ""));
	        Matcher  m = pattern.matcher(str);
	        if(m.matches()){
	            return m.group(1);
	        }
	        return null;
		}
		return null;
		
	}
	@Override
	public Map<String, Object> mapProcess(long offset, String row, 
			Mapper<LongWritable, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos) throws Exception{
		if (SequenceFileInputFormat.class.isAssignableFrom(context.getInputFormatClass())){
			int index = row.indexOf("\n");
			String pathName = row.substring(0,index);
			String tfName = getTableNameSetPathFileName(pathName);
			
			InputSplit inputSplit = context.getInputSplit();
			String fileName = ((FileSplit) inputSplit).getPath().getName(); 
			String line = row.substring(index+1);
			List<Tuple2<String, String>> ret = flatMapToPair(fileName, line, context);
			if (ret!=null) {
				for (Tuple2<String, String> t:ret){
					context.write(new Text(t._1), new Text(t._2));
				}
			}
		}else{
			String pathName = null;
			//process skip header
			if (skipHeader && offset==0) {
				logger.debug("skip header:" + row);
				return null;
			}
			pathName = ((FileSplit) context.getInputSplit()).getPath().toString();
			String tfName = getTableNameSetPathFileName(pathName);
			//process skip header exp
			if (skipHeaderCS!=null && offset==0){
				boolean skip = (boolean) ScriptEngineUtil.evalObject(skipHeaderCS, super.getSystemVariables());
				if (skip){
					logger.debug("skip header:" + row);
					return null;
				}
			}
			List<Tuple2<String, String>> ret = flatMapToPair(tfName, row, context);
			if (ret!=null) {
				for (Tuple2<String, String> t:ret){
					context.write(new Text(t._1), new Text(t._2));
				}
			}
		}
		return null;
	}

	@Override
	public List<Tuple2<String, String>> flatMapToPair(String tableName, String value,
			Mapper<LongWritable, Text, Text, Text>.Context context) throws Exception {
		super.init();
		List<Tuple2<String, String>> retList = new ArrayList<Tuple2<String, String>>();
		List<Tuple2<String, String>> csvList = parseXml2Values(tableName, value, context);
		if (context!=null){
			for(Tuple2<String, String> item: csvList){
				context.write(new Text(item._1), new Text(item._2));
			}
		}else{
			retList.addAll(csvList);
			return retList;
		}
		return null;
	}

	protected List<Tuple2<String, String>> parseXml2Values(String key, String text,
			Mapper<LongWritable, Text, Text, Text>.Context context) {
		
		try {
			List<Tuple2<String, String>> csvList = new ArrayList<Tuple2<String, String>>();
			Document dc = DocumentHelper.parseText(text);
			List<Element> elemNodeList = dc.selectNodes(nodeRootPath);
			Map<String, String> attrIdNameMap =logicSchema.getAttrIdNameMap();
			for (Element root:elemNodeList) {
				
				for (int i = 0; i < tables.length; i++) {
					String tableName = tables[i];
					HashMap<String, String> orgValueMap = new HashMap<>();
					XmlTableProperties tablePro = tablePropertiesMap.get(tableName);
					List<String> parentList = tablePro.getParentNodesXpath();
					String useFileNameAs = tablePro.getUseFileName();
					boolean isPDE4G = tablePro.isPde4G();
					if(useFileNameAs != null){
						orgValueMap.put(useFileNameAs, key);
					}
					
					for(String node:parentList){
						Node elem =  root.selectSingleNode(node);
						if(elem == null){
							continue;
						}
						orgValueMap.put(attrIdNameMap.get(elem.getPath()), elem.getText());
					}
					
					List<Element> eList =  root.selectNodes(tablePro.getStartNodeXpath());
					Map<String,String> stringFormatMap = tablePro.getFormatTimeMap();
					for(Element e: eList){ 
						// init map list
						List<HashMap<String, String>> mapList = new ArrayList<HashMap<String, String>>();
						HashMap<String, String> dataMap =  (HashMap<String, String>) orgValueMap.clone();
						mapList.add(dataMap);
						//if self is leafs 
						if(tablePro.getLeafsNodesXpath().contains("./"+e.getPath(e))){
							dataMap.put(attrIdNameMap.get(e.getPath()), "'"+formatElementValue(e.asXML()).replace(" ", "")+"'");
						}else{
							getNodesValue(e, e,tablePro,mapList);
						}
						List<String> orgAttrs = logicSchema.getAttrNames(tableName);
						for(HashMap<String, String> map:mapList){
							int length = orgAttrs.size();
							String[] vs = new String[length];
							for(int j=0; j< length; j++){
								String fieldName = orgAttrs.get(j);
								String strFormat = stringFormatMap.get(fieldName);
								if(strFormat != null){
									vs[j] = formatString(map.get(fieldName),strFormat);
								}else{
									vs[j] = map.get(fieldName);
								}
							}
							if(isPDE4G ){
								vs[vs.length-1] = getTTFF(string2Time(vs[15]), string2Time(vs[2]));
							}
							String csv = Util.getCsv(Arrays.asList(vs), false);
							csvList.add(new Tuple2<String, String>(tableName, csv));
						}
					}
					
				}
			}
			return csvList;
		} catch (DocumentException e) {
			logger.error(e.getMessage());
		}
		return null;
	}
	
	
	public  void getNodesValue(Element elem,Element originalNode,XmlTableProperties tablePro,List<HashMap<String, String>> mapList) {
		if(elem == null){
			return;
		}
		Map<String,String> fieldNameMap = logicSchema.getAttrIdNameMap(tablePro.getTableName());
		int length = mapList.size();
		String path = elem.getPath();
		String textV = formatElementValue(elem.getTextTrim());
		for (int i = 0; i < length; i++) {
			mapList.get(i).put(fieldNameMap.get(path), textV);
		}
		

		List<Attribute> listAttr = elem.attributes();
		for (Attribute attr : listAttr) {
			String attName = attr.getName().toLowerCase();
			for (int i = 0; i < length; i++) {
				mapList.get(i).put(fieldNameMap.get(path+"/@"+attName),formatElementValue(attr.getValue()));
			}
		}
		List<Element> listElement = elem.elements();
		
		int lastListIndex = -1; //deal neibour list
		List<HashMap<String, String>> cloneList = null;
		for (Element e : listElement) {  
			if(tablePro.getSkipNodesXpath().contains("./"+e.getPath(originalNode))) {
				//skip all below elements
				return;
			}
			int listIndex = isListElement(e.getUniquePath());
			if(listIndex > 0){
				logger.info(e.getPath());
				//add to list 
				if(listIndex == 1){
					if(lastListIndex != listIndex){
						cloneList = new ArrayList<HashMap<String, String>>();
						for (int i = 0; i < length; i++) {
							cloneList.add((HashMap<String, String>) mapList.get(i).clone());
						}
						lastListIndex = listIndex;
					}
				}else{
					if(lastListIndex != listIndex){
						for (int i = 0; i < length; i++) {
							mapList.add((HashMap<String, String>) cloneList.get(i).clone());
						}
						lastListIndex = listIndex;
					}
				}
				int start = length*(listIndex-1);
				for (int i = 0; i < length; i++) {
					if(tablePro.getLeafsNodesXpath().contains("./"+e.getPath(originalNode))){
						mapList.get(start+i).put(fieldNameMap.get(e.getPath()),"'"+formatElementValue(e.asXML())+"'");
					}else{
						getListNodeValues(e,originalNode,tablePro.getLeafsNodesXpath(),tablePro.getSkipNodesXpath(),mapList.get(start+i));
					}
				}
			
			}else{
				if(tablePro.getLeafsNodesXpath().contains("./"+e.getPath(originalNode))){
					for (int i = 0; i < length; i++) {
						mapList.get(i).put(fieldNameMap.get(e.getPath()),"'"+formatElementValue(e.asXML())+"'");
					}
				}else if(tablePro.getSkipNodesXpath().contains("./"+e.getPath(originalNode))) {
					//skip all below elements
				}else{
					getNodesValue(e,originalNode,tablePro,mapList);
				}
			}
		}
		
		
	}
	
	
	private  void getListNodeValues(Element elem,Element originalNode,List<String> stopList,List<String> skipList,HashMap<String, String> valueMap){
		Map<String, String> attrIdNameMap = logicSchema.getAttrIdNameMap();
		valueMap.put(attrIdNameMap.get(elem.getPath()),formatElementValue(elem.getTextTrim()));
		List<Attribute> listAttr = elem.attributes();
		for (Attribute attr : listAttr) {
			String attName = attr.getName().toLowerCase();
			valueMap.put(attrIdNameMap.get(elem.getPath()+"/@"+attName),formatElementValue(attr.getValue()));
			
		}
		
		List<Element> listElement = elem.elements();
		for (Element e : listElement) {  
			if(isListElement(e.getUniquePath()) > 0){
				continue;
			}
			if(stopList.contains("./"+e.getPath(originalNode))){
				valueMap.put(attrIdNameMap.get(e.getPath()),"'"+formatElementValue(e.asXML())+"'");
			}else if(skipList.contains("./"+e.getPath(originalNode))) {
				//skip all below elements
			}else{
				getListNodeValues(e,originalNode,stopList,skipList,valueMap);
			}
		}
	
	}
	
	private static int isListElement(String str){
		Pattern pattern = Pattern.compile("[\\s\\S]*\\[(\\d+)]");
        Matcher  m = pattern.matcher(str);
        if(m.matches()){
            return Integer.valueOf(m.group(1));
        }
        return -1;
	}
	
	private String formatElementValue(String str){
		if(str != null){
			return str.replace(",", "\\,").replace("\n", " ");
		}
		return null;
	}
	

	@Override
	public List<Tuple3<String, String, String>> reduceByKey(String key, Iterable<? extends Object> values, 
			Reducer<Text, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos) throws Exception{
	/*	List<Tuple3<String,String,String>> out = new ArrayList<Tuple3<String,String,String>>();
		for (Object v: values)
			out.add(new Tuple3<String,String,String>(v.toString(), null, key));
		return out;*/
		String pathName = key.toString();
		int lastSep = pathName.lastIndexOf("/");
		String fileName = pathName.substring(lastSep+1);
		Iterator<? extends Object> it = values.iterator();
		List<Tuple3<String, String, String>> ret = new ArrayList<Tuple3<String, String, String>>();
		while (it.hasNext()){
			String v = it.next().toString();
			String tableName = ETLCmd.SINGLE_TABLE;
			if (super.getOutputType()==OutputType.multiple){
				tableName = fileName.toString();
			}
			if (context!=null){//map reduce
				EngineUtil.processReduceKeyValue(v, null, tableName, context, mos);
			}else{
				ret.add(new Tuple3<String, String, String>(v, null, tableName));
			}
		}
		return ret;
	}
	
	public static Date string2Time(String timeStr){
		if(timeStr == null || timeStr.equals("")){
			return null;
		}
		 try {
			return FieldType.sdatetimeFormat.parse(timeStr);
		} catch (ParseException e) {
			logger.error(e.getMessage());
		}
		return null;
	}
	private String getTTFF(Date endTime,Date startTime){
		if(endTime == null || startTime == null){
			return null;
		}
		return (endTime.getTime()-startTime.getTime()) +"";
	}
}
