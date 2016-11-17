package etl.flow.spark;

import etl.flow.mgr.FlowServerConf;

public class SparkConf implements FlowServerConf {
	
	private String tmpFolder;
	

	public String getTmpFolder() {
		return tmpFolder;
	}
	public void setTmpFolder(String tmpFolder) {
		this.tmpFolder = tmpFolder;
	}

}
