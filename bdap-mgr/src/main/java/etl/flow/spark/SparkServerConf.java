package etl.flow.spark;

import org.apache.commons.configuration.PropertiesConfiguration;

import bdap.util.PropertiesUtil;
import etl.flow.mgr.FlowServerConf;
import etl.flow.oozie.OozieConf;

public class SparkServerConf implements FlowServerConf {
	
	private static final String key_tmp_folder="tmp.dir";
	private static final String key_jdk_bin="jdk.bin";
	
	private String tmpFolder;
	private String srcFolder="src";
	private String classesFolder="classes";
	private String targetFolder="target";
	private String jdkBin;
	
	public SparkServerConf(String confFile){
		PropertiesConfiguration pc = PropertiesUtil.getPropertiesConfig(confFile);
		tmpFolder = pc.getString(key_tmp_folder);
		jdkBin = pc.getString(key_jdk_bin);
	}
	
	public String getTmpFolder() {
		return tmpFolder;
	}
	public void setTmpFolder(String tmpFolder) {
		this.tmpFolder = tmpFolder;
	}

	public String getSrcFolder() {
		return srcFolder;
	}

	public String getClassesFolder() {
		return classesFolder;
	}

	public String getTargetFolder() {
		return targetFolder;
	}

	public void setSrcFolder(String srcFolder) {
		this.srcFolder = srcFolder;
	}

	public void setClassesFolder(String classesFolder) {
		this.classesFolder = classesFolder;
	}

	public void setTargetFolder(String targetFolder) {
		this.targetFolder = targetFolder;
	}

	public String getJdkBin() {
		return jdkBin;
	}

	public void setJdkBin(String jdkBin) {
		this.jdkBin = jdkBin;
	}

}
