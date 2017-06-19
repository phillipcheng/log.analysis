package etl.cmd.dynschema;

import java.util.List;

import etl.util.FieldType;

public class DynamicTableSchema {
	
	private String id;
	private String name;
	private List<String> fieldNames;
	private List<String> fieldIds;
	private String[] valueSample;//used to guess type, if type is not known before hand
	private List<FieldType> types;//if this is not set, guess it from values
	private List<String> valueList;
	
	
	public DynamicTableSchema(){
	}
	
	//values can be null
	public DynamicTableSchema(String name, List<String> fieldNames, String[] values, List<FieldType> types){
		this.name = name;
		this.fieldNames = fieldNames;
		this.valueSample = values;
		this.types = types;
	}
	
	public DynamicTableSchema(String id, String name, List<String> fieldIds, List<String> fieldNames, List<FieldType> types){
		this(name, fieldNames, null, types);
		this.types = types;
		this.id=id;
		this.fieldIds=fieldIds;
	}
	
	public String getName() {
		return name;
	}
	public List<String> getFieldNames() {
		return fieldNames;
	}
	public void setName(String name) {
		this.name = name;
	}
	public void setFieldNames(List<String> fieldNames) {
		this.fieldNames = fieldNames;
	}
	public List<FieldType> getTypes() {
		return types;
	}

	public void setTypes(List<FieldType> types) {
		this.types = types;
	}

	public String[] getValueSample() {
		return valueSample;
	}

	public void setValueSample(String[] valueSample) {
		this.valueSample = valueSample;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<String> getFieldIds() {
		return fieldIds;
	}

	public void setFieldIds(List<String> fieldIds) {
		this.fieldIds = fieldIds;
	}

	public List<String> getValueList() {
		return valueList;
	}

	public void setValueList(List<String> valueList) {
		this.valueList = valueList;
	}
	
	public void copyValueListToArray(){
		valueSample = valueList.toArray(new String[]{});
		valueList.clear();
	}

}
