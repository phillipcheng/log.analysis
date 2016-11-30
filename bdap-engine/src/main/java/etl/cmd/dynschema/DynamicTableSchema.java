package etl.cmd.dynschema;

import java.util.List;

import etl.util.FieldType;

public class DynamicTableSchema {
	
	private String name;
	private List<String> fieldNames;
	private String[] valueSample;
	private List<FieldType> types;//if this is not set, guess it from values
	
	public DynamicTableSchema(String name, List<String> fieldNames, String[] values){
		this.name = name;
		this.fieldNames = fieldNames;
		this.setValueSample(values);
	}
	
	public DynamicTableSchema(String name, List<String> fieldNames, String[] values, List<FieldType> types){
		this(name, fieldNames, values);
		this.types = types;
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

}
