package etl.util;

import java.util.List;
import java.util.Map;

public class XmlTableProperties {

	private String tableName; //table name
	
	//table configure
	private String startNodeXpath;
	private List<String> leafsNodesXpath;
	private List<String> parentNodesXpath;
	private List<String> skipNodesXpath;
	private Map<String,String> formatTimeMap ;//format time string
	private String useFileName; // filename as field
	private boolean pde4G;
	
	
	public XmlTableProperties(String tableName, String startNodeXpath, List<String> stopNodesXpath,
			List<String> parentNodesXpath,List<String> skipNodesXpath) {
		this.tableName = tableName;
		this.startNodeXpath = startNodeXpath;
		this.leafsNodesXpath = stopNodesXpath;
		this.parentNodesXpath = parentNodesXpath;
		this.skipNodesXpath =skipNodesXpath;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getStartNodeXpath() {
		return startNodeXpath;
	}
	public void setStartNodeXpath(String startNodeXpath) {
		this.startNodeXpath = startNodeXpath;
	}

	public List<String> getLeafsNodesXpath() {
		return leafsNodesXpath;
	}
	public void setLeafsNodesXpath(List<String> leafsNodesXpath) {
		this.leafsNodesXpath = leafsNodesXpath;
	}
	public List<String> getParentNodesXpath() {
		return parentNodesXpath;
	}
	public void setParentNodesXpath(List<String> parentNodesXpath) {
		this.parentNodesXpath = parentNodesXpath;
	}
	public List<String> getSkipNodesXpath() {
		return skipNodesXpath;
	}
	public void setSkipNodesXpath(List<String> skipNodesXpath) {
		this.skipNodesXpath = skipNodesXpath;
	}
	public Map<String, String> getFormatTimeMap() {
		return formatTimeMap;
	}
	public void setFormatTimeMap(Map<String, String> formatTimeMap) {
		this.formatTimeMap = formatTimeMap;
	}
	public String getUseFileName() {
		return useFileName;
	}
	public void setUseFileName(String useFileName) {
		this.useFileName = useFileName;
	}
	public boolean isPde4G() {
		return pde4G;
	}
	public void setPde4G(boolean pde4g) {
		pde4G = pde4g;
	}
	
}
