package etl.cmd.dynschema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.Node;

import etl.engine.LogicSchema;
import etl.engine.types.ProcessMode;
import etl.util.ConfigKey;
import etl.util.FieldType;
import etl.util.XmlTableProperties;

public class Xml2SchemaCmd extends DynamicXmlSchemaCmd{
	private static final long serialVersionUID = 1L;
	
	public static final Logger logger = LogManager.getLogger(Xml2SchemaCmd.class);
	
	public static final @ConfigKey String cfgkey_node_root_path="node.root.path";
	public static final @ConfigKey String cfgkey_tables="tables";
	public static final @ConfigKey String cfgkey_node_start=".node.start";
	public static final @ConfigKey String cfgkey_node_leafs=".node.leafs";
	public static final @ConfigKey String cfgkey_node_skip=".node.skip";
	public static final @ConfigKey String cfgkey_node_associate=".node.associate";
	public static final @ConfigKey String cfgkey_table_common_element="common.node.associate";
	
	private String nodeRootPath;
	private String[] tables;
	private transient Map<String, XmlTableProperties> tablePropertiesMap;
	private HashMap<String,HashMap<String,String>> sameNameMaps;
	
	
	public Xml2SchemaCmd(){
	}
	
	public Xml2SchemaCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	public Xml2SchemaCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs, ProcessMode pm){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, pm);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs, ProcessMode pm){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
		nodeRootPath = super.getCfgString(cfgkey_node_root_path,null);
		tables = super.getCfgStringArray(cfgkey_tables);
		sameNameMaps = new HashMap<String,HashMap<String,String>>();
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
			tablePropertiesMap.put(table, tableProperty);
			sameNameMaps.put(table, new HashMap<String,String>());
		}
		
	}

	
	@Override
	public List<DynamicTableSchema> getDynamicTable(String input, LogicSchema ls) throws Exception{
		List<DynamicTableSchema> dtList = new ArrayList<DynamicTableSchema>();
		Document dc = DocumentHelper.parseText(input);
		List<Element> elemNodeList = dc.selectNodes(nodeRootPath);
		for (Element root:elemNodeList) {
			
			for (int i = 0; i < tables.length; i++) {
				List<String> fieldIdList = new ArrayList<String>();
				List<String> fieldNameList = new ArrayList<String>();
				List<String> fieldValueList = new ArrayList<String>();
				List<FieldType> typelist = new ArrayList<FieldType>();
				HashMap<String,String> sameMap = new HashMap<String,String>();
				DynamicTableSchema dtable = new DynamicTableSchema();
				dtable.setName(tables[i]);
				dtable.setFieldIds(fieldIdList);
				dtable.setFieldNames(fieldNameList);
				dtable.setTypes(typelist);
				dtable.setValueList(fieldValueList);
				XmlTableProperties tablePro = tablePropertiesMap.get(tables[i]);
				List<String> parentList = tablePro.getParentNodesXpath();
				for(String node:parentList){
					Node elem =  root.selectSingleNode(node);
					if(elem == null){
						continue;
					}
					String fieldName = formatString(elem.getName());
					String[] strs = node.split("/");
					if(strs[strs.length-1].startsWith("@")){ //attribute
						fieldName = formatString(strs[strs.length-2])+"_Attri_"+fieldName;
					}
					if(!dtable.getFieldNames().contains(fieldName)){
						dtable.getFieldNames().add(fieldName);
						dtable.getFieldIds().add(elem.getPath());
						dtable.getValueList().add(elem.getText());
					}
//					Element elem = (Element) root.selectSingleNode(node);
//					if(elem == null){
//						continue;
//					}
//					String fieldName = formatString(elem.getName());
//					setDynamicSchema(fieldName,elem,dtable,1);
				}
				
				/*	List<Element> elems =  document.selectNodes(tablePro.getStartNodeXpath());
			for(Element elem: elems){
				getNodes(elem, elem, tablePro.getLeafsNodesXpath(), tablePro.getSkipNodesXpath(), dtable);
			}*/
				Element elem =  (Element) root.selectSingleNode(tablePro.getStartNodeXpath());
				if(elem != null){
					getNodes(elem, elem, tablePro.getLeafsNodesXpath(), tablePro.getSkipNodesXpath(), dtable);
				}
				
				
				dtable.copyValueListToArray();
				dtList.add(dtable);
			}
			
		}
		return dtList;
	}
	
	private void setDynamicSchema(String fieldName, Element elem,DynamicTableSchema dtable,int leafType){
		
		if(leafType == 1){
			if(!isGroupNode(elem)){
				if(!dtable.getFieldNames().contains(fieldName) && elem.getTextTrim() != null && !elem.getTextTrim().equals("")){
					dtable.getFieldNames().add(fieldName);
					dtable.getFieldIds().add(elem.getPath());
					dtable.getValueList().add(elem.getTextTrim());
				}
			}
			List<Attribute> listAttr = elem.attributes();
			for (Attribute attr : listAttr) {
				String attName = attr.getName().toLowerCase();
				if(!dtable.getFieldNames().contains(fieldName+"_Attri_"+attName)){
					dtable.getFieldIds().add(attr.getPath());
					dtable.getFieldNames().add(fieldName+"_Attri_"+attName);
					dtable.getValueList().add(attr.getValue());
				}
				
			}
		}else if(leafType == 2){
			if(!dtable.getFieldNames().contains(fieldName)){
				dtable.getFieldNames().add(fieldName);
				dtable.getFieldIds().add(elem.getPath());
				dtable.getValueList().add(elem.asXML().replace("\n", ""));
			}
		}
	}
	
	
	private void removeOldSameEleInfo(Element oldElem,DynamicTableSchema dtable){
		
		if(oldElem != null){
			String oldEName = formatString(oldElem.getName());
			
			int oldIndex = dtable.getFieldNames().indexOf(oldEName);
			if(oldIndex > 0){
				dtable.getFieldIds().remove(oldIndex);
				dtable.getFieldNames().remove(oldIndex);
				dtable.getValueList().remove(oldIndex);
			}
			List<Attribute> listAttr = oldElem.attributes();
			for (Attribute attr : listAttr) {
				String attName = attr.getName().toLowerCase();
				int oldAttrIndex = dtable.getFieldNames().indexOf(oldEName+"_Attri_"+attName);
				if(oldAttrIndex > 0){
					dtable.getFieldIds().remove(oldAttrIndex);
					dtable.getFieldNames().remove(oldAttrIndex);
					dtable.getValueList().remove(oldAttrIndex);
				}
			}
		}
	}
	
	public  void getNodes(Element elem,Element originalNode,List<String> stopList,List<String> skipList,DynamicTableSchema dtable) {
		String fieldName = formatString(elem.getName());
		int leafType = 1;
		if(stopList.contains("./"+elem.getPath(originalNode))){
			leafType = 2;
		}else if(skipList.contains("./"+elem.getPath(originalNode))){
			//skil all below elements
			leafType = 3;
			return;
		}else{
			leafType = 1;
		}
		
		int index = dtable.getFieldNames().indexOf(fieldName);
		if(index == -1){ // no equals element name
			if(sameNameMaps.get(dtable.getName()).get(fieldName) != null){
				String[] newPaths = elem.getPath().split("/");
				if( newPaths.length >= 3){
					fieldName = formatString(newPaths[newPaths.length-1]) + "_4_" + formatString(newPaths[newPaths.length-2])+ "_4_" + formatString(newPaths[newPaths.length-3]);
				}
			}
			setDynamicSchema(fieldName,elem,dtable,leafType);
		}else{ //there are two same name
			//change the two same name name_4_parent
			String oldPath = dtable.getFieldIds().get(index); // oldPath
			if(oldPath.equals(elem.getPath())){  //this is list item so do nothing
				return;
			}else{
				logger.info("fieldName " + elem.getPath());
				sameNameMaps.get(dtable.getName()).put(fieldName, fieldName);
//				sameNameList.add(fieldName);
				//remove old elem info 
//				String oldPath = dtable.getFieldIds().get(index); 
				Element oldElem = (Element) originalNode.selectSingleNode(oldPath);
				removeOldSameEleInfo(oldElem,dtable);
				int leafTypeOld = 1;
				if(stopList.contains("./"+oldElem.getPath(originalNode))){
					leafTypeOld = 2;
				}else if(skipList.contains("./"+oldElem.getPath(originalNode))){
					//skil all below elements
					leafTypeOld = 3;
//					return;
				}else{
					leafTypeOld = 1;
				}
//				logger.warn("there are same elements oldPaht: " + oldPath +"; newPath:"+ elem.getPath());
				
				//hand the same name
				String[] oldPaths = oldPath.split("/");
				String[] newPaths = elem.getPath().split("/");
				if(oldPaths.length >= 3 && newPaths.length >= 3){
					
					String oldFieldName = formatString(oldPaths[oldPaths.length-1]) + "_4_" + formatString(oldPaths[oldPaths.length-2])+ "_4_" + formatString(oldPaths[oldPaths.length-3]);
					setDynamicSchema(oldFieldName,oldElem,dtable,leafTypeOld);
					fieldName = formatString(newPaths[newPaths.length-1]) + "_4_" + formatString(newPaths[newPaths.length-2])+ "_4_" + formatString(newPaths[newPaths.length-3]);
					setDynamicSchema(fieldName,elem,dtable,leafType);
					
				}else if(oldPaths.length >= 2 && newPaths.length >= 2){ // don't care oldPath is root elem
						if(!formatString(oldPaths[oldPaths.length-2]).equals(formatString(newPaths[newPaths.length -2]))){
						
							String oldFieldName = formatString(oldPaths[oldPaths.length-1]) + "_4_" + formatString(oldPaths[oldPaths.length-2]);
							setDynamicSchema(oldFieldName,oldElem,dtable,leafTypeOld);
							fieldName = formatString(newPaths[newPaths.length-1]) + "_4_" + formatString(newPaths[newPaths.length-2]);
							setDynamicSchema(fieldName,elem,dtable,leafType);
						}else{
							if(oldPaths.length >= 3){
								String oldFieldName = formatString(oldPaths[oldPaths.length-1]) + "_4_" + formatString(oldPaths[oldPaths.length-2])+ "_4_" + formatString(oldPaths[oldPaths.length-3]);
								setDynamicSchema(oldFieldName,oldElem,dtable,leafTypeOld);
							}
							if(newPaths.length >= 3){
								fieldName = formatString(newPaths[newPaths.length-1]) + "_4_" + formatString(newPaths[newPaths.length-2])+ "_4_" + formatString(newPaths[newPaths.length-3]);
								setDynamicSchema(fieldName,elem,dtable,leafType);
							}
						}
				}
				
			}
				
		}
		if(leafType != 1){
			return;
		}
		
		List<Element> listElement = elem.elements();
		for (Element e : listElement) {
			getNodes(e,originalNode,stopList,skipList,dtable);
		}
		
	}

	private static boolean isInList(String str){
		Pattern pattern = Pattern.compile("\\[(\\d+)]");
        Matcher  m = pattern.matcher(str);
        while(m.find()){
            if(Integer.valueOf(m.group(1)) > 1){
            	return true;
            }
        }
        return false;
	}
	
	private boolean isGroupNode(Element elem){
		List<Element> listElement = elem.elements();
		if(listElement.size() > 0 ){
			return true;
		}
		return false;
	}
	


}
