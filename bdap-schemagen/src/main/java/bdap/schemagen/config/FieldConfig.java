package bdap.schemagen.config;

public class FieldConfig {
	private int index = -1;
	private String headerColumnName;
	private int idIndex = -1;
	private String idHeaderColumnName;
	private int fieldTypeIndex = -1;
	private String fieldTypeHeaderColumnName;
	private int fieldSizeIndex = -1;
	private String fieldSizeHeaderColumnName;
	private int fieldAggrTypeIndex = -1;
	private String fieldAggrTypeHeaderColumnName;
	private String defaultFieldType;
	private int defaultFieldSize;
	private boolean multiple;
	private String multipleSeparator;
	private int multipleFixedCount;
	private String multipleDefaultNamePrefix;

	public int getIndex() {
		return index;
	}
	public void setIndex(int index) {
		this.index = index;
	}
	public String getHeaderColumnName() {
		return headerColumnName;
	}
	public void setHeaderColumnName(String headerColumnName) {
		this.headerColumnName = headerColumnName;
	}
	public int getIdIndex() {
		return idIndex;
	}
	public void setIdIndex(int idIndex) {
		this.idIndex = idIndex;
	}
	public String getIdHeaderColumnName() {
		return idHeaderColumnName;
	}
	public void setIdHeaderColumnName(String idHeaderColumnName) {
		this.idHeaderColumnName = idHeaderColumnName;
	}
	public int getFieldTypeIndex() {
		return fieldTypeIndex;
	}
	public void setFieldTypeIndex(int fieldTypeIndex) {
		this.fieldTypeIndex = fieldTypeIndex;
	}
	public String getFieldTypeHeaderColumnName() {
		return fieldTypeHeaderColumnName;
	}
	public void setFieldTypeHeaderColumnName(String fieldTypeHeaderColumnName) {
		this.fieldTypeHeaderColumnName = fieldTypeHeaderColumnName;
	}
	public int getFieldSizeIndex() {
		return fieldSizeIndex;
	}
	public void setFieldSizeIndex(int fieldSizeIndex) {
		this.fieldSizeIndex = fieldSizeIndex;
	}
	public String getFieldSizeHeaderColumnName() {
		return fieldSizeHeaderColumnName;
	}
	public void setFieldSizeHeaderColumnName(String fieldSizeHeaderColumnName) {
		this.fieldSizeHeaderColumnName = fieldSizeHeaderColumnName;
	}
	public int getFieldAggrTypeIndex() {
		return fieldAggrTypeIndex;
	}
	public void setFieldAggrTypeIndex(int fieldAggrTypeIndex) {
		this.fieldAggrTypeIndex = fieldAggrTypeIndex;
	}
	public String getFieldAggrTypeHeaderColumnName() {
		return fieldAggrTypeHeaderColumnName;
	}
	public void setFieldAggrTypeHeaderColumnName(String fieldAggrTypeHeaderColumnName) {
		this.fieldAggrTypeHeaderColumnName = fieldAggrTypeHeaderColumnName;
	}
	public String getDefaultFieldType() {
		return defaultFieldType;
	}
	public void setDefaultFieldType(String defaultFieldType) {
		this.defaultFieldType = defaultFieldType;
	}
	public int getDefaultFieldSize() {
		return defaultFieldSize;
	}
	public void setDefaultFieldSize(int defaultFieldSize) {
		this.defaultFieldSize = defaultFieldSize;
	}
	public boolean isMultiple() {
		return multiple;
	}
	public void setMultiple(boolean multiple) {
		this.multiple = multiple;
	}
	public String getMultipleSeparator() {
		return multipleSeparator;
	}
	public void setMultipleSeparator(String multipleSeparator) {
		this.multipleSeparator = multipleSeparator;
	}
	public int getMultipleFixedCount() {
		return multipleFixedCount;
	}
	public void setMultipleFixedCount(int multipleFixedCount) {
		this.multipleFixedCount = multipleFixedCount;
	}
	public String getMultipleDefaultNamePrefix() {
		return multipleDefaultNamePrefix;
	}
	public void setMultipleDefaultNamePrefix(String multipleDefaultNamePrefix) {
		this.multipleDefaultNamePrefix = multipleDefaultNamePrefix;
	}
}
