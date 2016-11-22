package bdap.schemagen.config;

import java.util.Map;

public class Config {
	private ItemConfig tableId;
	private ItemConfig tableName;
	private FieldConfig[] fields;
	private Map<String, String> fieldTypeMapping;
	public ItemConfig getTableId() {
		return tableId;
	}
	public void setTableId(ItemConfig tableId) {
		this.tableId = tableId;
	}
	public ItemConfig getTableName() {
		return tableName;
	}
	public void setTableName(ItemConfig tableName) {
		this.tableName = tableName;
	}
	public FieldConfig[] getFields() {
		return fields;
	}
	public void setFields(FieldConfig[] fields) {
		this.fields = fields;
	}
	public Map<String, String> getFieldTypeMapping() {
		return fieldTypeMapping;
	}
	public void setFieldTypeMapping(Map<String, String> fieldTypeMapping) {
		this.fieldTypeMapping = fieldTypeMapping;
	}
}
