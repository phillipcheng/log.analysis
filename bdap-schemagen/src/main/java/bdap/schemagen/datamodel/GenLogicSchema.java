package bdap.schemagen.datamodel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import etl.engine.LogicSchema;

public class GenLogicSchema extends LogicSchema {
	private static final long serialVersionUID = 1L;

	private Map<String, List<String>> attrIdMap = null; //table-name to list of attribute ids mapping
	
	public GenLogicSchema() {
		attrIdMap = new HashMap<String, List<String>>();
	}

	public Map<String, List<String>> getAttrIdMap() {
		return attrIdMap;
	}

	public void setAttrIdMap(Map<String, List<String>> attrIdMap) {
		this.attrIdMap = attrIdMap;
	}
}
