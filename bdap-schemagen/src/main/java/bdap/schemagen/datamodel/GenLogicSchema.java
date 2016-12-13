package bdap.schemagen.datamodel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;

import etl.engine.LogicSchema;

public class GenLogicSchema extends LogicSchema {
	private static final long serialVersionUID = 1L;

	private Map<String, List<String>> attrIdMap = null; //table-name to list of attribute ids mapping
	private transient Map<String, String> tableNameIdMap = null; //reverse mapping test name duplication
	
	public GenLogicSchema() {
		attrIdMap = new HashMap<String, List<String>>();
		tableNameIdMap = new HashMap<String, String>();
	}

	public Map<String, List<String>> getAttrIdMap() {
		return attrIdMap;
	}

	public void setAttrIdMap(Map<String, List<String>> attrIdMap) {
		this.attrIdMap = attrIdMap;
	}
	
	@JsonIgnore
	public Map<String, String> getTableNameIdMap() {
		return tableNameIdMap;
	}
	
	public void setTableNameIdMap(Map<String, String> tableNameIdMap) {
		this.tableNameIdMap = tableNameIdMap;
	}
}
