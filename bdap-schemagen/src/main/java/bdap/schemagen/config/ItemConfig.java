package bdap.schemagen.config;

public class ItemConfig {
	private int index = -1;
	private String headerColumnName;
	private String comparisonMethod;
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
	public String getComparisonMethod() {
		return comparisonMethod;
	}
	public void setComparisonMethod(String comparisonMethod) {
		this.comparisonMethod = comparisonMethod;
	}
}
