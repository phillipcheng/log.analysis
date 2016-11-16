package etl.engine;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonIgnore;

import etl.util.FieldType;


public class LogicSchema implements Serializable{
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(LogicSchema.class);
	
	private Map<String, List<String>> attrNameMap = null; //table-name to list of attribute names mapping
	private Map<String, List<FieldType>> attrTypeMap = null; //table-name to list of attribute types mapping

	public LogicSchema(){
		attrNameMap = new HashMap<String, List<String>>();
		attrTypeMap = new HashMap<String, List<FieldType>>();
	}
	
	@Override
	public boolean equals(Object obj){
		if (!(obj instanceof LogicSchema)){
			return false;
		}
		LogicSchema that = (LogicSchema) obj;
		if (!Objects.equals(attrNameMap, that.getAttrNameMap())){
			return false;
		}
		if (!Objects.equals(attrTypeMap, that.getAttrTypeMap())){
			return false;
		}
		return true;
	}
	
	public boolean hasTable(String tableName){
		return attrNameMap.containsKey(tableName);
	}
	
	@JsonIgnore
	public List<String> getAttrNames(String tableName){
		return attrNameMap.get(tableName);
	}
	
	public void addAttributes(String tableName, List<String> attributes){
		List<String> originAttr = attrNameMap.get(tableName);
		if (originAttr==null){
			attrNameMap.put(tableName, attributes);
		}else{
			originAttr.addAll(attributes);
			attrNameMap.put(tableName, originAttr);
		}
	}
	
	public void updateTableAttrs(String tableName, List<String> attributes){
		attrNameMap.put(tableName, attributes);
	}
	
	@JsonIgnore
	public List<FieldType> getAttrTypes(String tableName){
		return attrTypeMap.get(tableName);
	}
	
	public void addAttrTypes(String tableName, List<FieldType> attrTypes){
		List<FieldType> orgTypes = attrTypeMap.get(tableName);
		if (orgTypes==null){
			attrTypeMap.put(tableName, attrTypes);
		}else{
			orgTypes.addAll(attrTypes);
			attrTypeMap.put(tableName, orgTypes);
		}
	}
	
	public void updateTableAttrTypes(String tableName, List<FieldType> attrTypes){
		attrTypeMap.put(tableName, attrTypes);
	}
	
	//for json serializer
	public Map<String, List<String>> getAttrNameMap() {
		return attrNameMap;
	}
	public void setAttrNameMap(Map<String, List<String>> attrNameMap) {
		this.attrNameMap = attrNameMap;
	}

	public Map<String, List<FieldType>> getAttrTypeMap() {
		return attrTypeMap;
	}

	public void setAttrTypeMap(Map<String, List<FieldType>> attrTypeMap) {
		this.attrTypeMap = attrTypeMap;
	}
}
