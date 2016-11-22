package bdap.schemagen.config;

public class FieldConfig {
	private int index = -1;
	private String headerColumnName;
	private int fieldTypeIndex = -1;
	private String fieldTypeHeaderColumnName;
	private int fieldSizeIndex = -1;
	private String fieldSizeHeaderColumnName;
	private String defaultFieldType;
	private int defaultFieldSize;
	private boolean multiple;
	private String multipleSeparator;

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
}
