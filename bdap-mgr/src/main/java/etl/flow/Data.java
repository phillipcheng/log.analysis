package etl.flow;

import java.util.Objects;

public class Data {
	
	private String name;
	private String location;//hdfs directory
	private String baseOutput;//baseOutput, for multiple outputs
	private String schemaName = null;//reference to schema
	private InputFormatType dataFormat = InputFormatType.File;//for processing
	private RecordType recordType = RecordType.String;//
	private PersistType psType = PersistType.FileOrMem;
	
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
	public Data(String name, String location, InputFormatType dataType, PersistType psType){
		this(name, location, dataType);
		this.setPsType(psType);
	}
	public Data(String name, String location, InputFormatType dataType, PersistType psType, RecordType recordType){
		this(name, location, dataType, psType);
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

	public PersistType getPsType() {
		return psType;
	}

	public void setPsType(PersistType psType) {
		this.psType = psType;
	}

	public String getBaseOutput() {
		return baseOutput;
	}

	public void setBaseOutput(String baseOutput) {
		this.baseOutput = baseOutput;
	}

	public RecordType getRecordType() {
		return recordType;
	}

	public void setRecordType(RecordType recordType) {
		this.recordType = recordType;
	}

}
