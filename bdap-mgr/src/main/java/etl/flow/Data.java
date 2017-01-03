package etl.flow;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;

import etl.engine.DataType;
import etl.engine.InputFormatType;

public class Data {
	
	public static final String INTANCE_FLOW_ME="me";
	
	private String name;
	private String location;//hdfs directory, if instance = true, only used by oozie
	private String baseOutput;//baseOutput, for multiple outputs, the key for the data
	private String schemaName = null;//reference to schema
	private InputFormatType dataFormat = InputFormatType.Text;//for processing
	private DataType recordType = DataType.Value;//for spark io conversion, also decide the inputformat
	private boolean instance = true; //if instance is true, the input path is location/$wfid
	private String instanceFlow = INTANCE_FLOW_ME;//if instance is true, which flow's instance, default to me
	
	private Map<String, Object> properties = new HashMap<String, Object>();
	
	public Data(){
	}
	
	public Data(String name, String location){
		this.name = name;
		this.location = location;
	}
	public Data(String name, String location, InputFormatType dataType){
		this(name, location);
		this.setDataFormat(dataType);
	}
	public Data(String name, String location, InputFormatType dataType, DataType recordType){
		this(name, location, dataType);
		this.recordType = recordType;
	}
	public Data(String name, String location, InputFormatType dataType, boolean instance){
		this(name, location, dataType);
		this.instance = instance;
	}
	public Data(String name, String location, InputFormatType dataType, DataType recordType, boolean instance){
		this(name, location, dataType, instance);
		this.recordType = recordType;
	}
	
	public String toString(){
		return String.format("%s, %s, %s, %s, %s, %b", name, location, dataFormat, recordType, schemaName, instance);
	}
	
	@Override
	public boolean equals(Object obj){
		if (!(obj instanceof Data)){
			return false;
		}
		Data that = (Data)obj;
		if (!Objects.equals(that.getName(), name)){
			return false;
		}
		if (!Objects.equals(that.getLocation(), location)){
			return false;
		}
		return true;
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}
	public boolean isInstance() {
		return instance;
	}
	public void setInstance(boolean instance) {
		this.instance = instance;
	}
	public String getSchemaName() {
		return schemaName;
	}
	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}
	public InputFormatType getDataFormat() {
		return dataFormat;
	}
	public void setDataFormat(InputFormatType dataFormat) {
		this.dataFormat = dataFormat;
	}
	public String getBaseOutput() {
		return baseOutput;
	}
	public void setBaseOutput(String baseOutput) {
		this.baseOutput = baseOutput;
	}
	public DataType getRecordType() {
		return recordType;
	}
	public void setRecordType(DataType recordType) {
		this.recordType = recordType;
	}
	@JsonAnySetter
	public void putProperty(String key, Object value){
		properties.put(key, value);
	}
	@JsonAnyGetter
	public Map<String, Object> getProperties() {
		return properties;
	}

	public String getInstanceFlow() {
		return instanceFlow;
	}
	public void setInstanceFlow(String instanceFlow) {
		this.instanceFlow = instanceFlow;
	}
}
