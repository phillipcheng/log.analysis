package etl.flow;

import java.util.Objects;

import etl.engine.DataType;
import etl.engine.InputFormatType;

public class Data {
	
	private String name;
	private String location;//hdfs directory, if instance = true, only used by oozie
	private String baseOutput;//baseOutput, for multiple outputs
	private String schemaName = null;//reference to schema
	private InputFormatType dataFormat = InputFormatType.Text;//for processing
	private DataType recordType = DataType.Value;//for spark io conversion
	
	private boolean instance = true; //if instance is true, the input path is location/$wfid
	
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

}
