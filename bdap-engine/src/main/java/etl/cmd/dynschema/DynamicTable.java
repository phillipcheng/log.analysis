package etl.cmd.dynschema;

import java.util.List;

import etl.util.FieldType;

public class DynamicTable {
	
	private String name;
	private List<String> fieldNames;
	private List<String[]> values;
	private List<FieldType> types;//if this is not set, guess it from values
	
	public DynamicTable(String name, List<String> fieldNames, List<String[]> values){
		this.name = name;
		this.fieldNames = fieldNames;
		this.values = values;
	}
	
	public DynamicTable(String name, List<String> fieldNames, List<String[]> values, List<FieldType> types){
		this(name, fieldNames, values);
		this.types = types;
	}
	
	public String getName() {
		return name;
	}
	public List<String> getFieldNames() {
		return fieldNames;
	}
	public List<String[]> getValues() {
		return values;
	}
	public void setName(String name) {
		this.name = name;
	}
	public void setFieldNames(List<String> fieldNames) {
		this.fieldNames = fieldNames;
	}
	public void setValues(List<String[]> values) {
		this.values = values;
	}

	public List<FieldType> getTypes() {
		return types;
	}

	public void setTypes(List<FieldType> types) {
		this.types = types;
	}

}
