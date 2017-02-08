package etl.flow.spark;

import org.apache.commons.configuration.PropertiesConfiguration;

import bdap.util.PropertiesUtil;
import etl.flow.mgr.FlowServerConf;
import etl.flow.oozie.OozieConf;

public class SparkServerConf implements FlowServerConf {
	
	private static final String key_tmp_folder="tmp.dir";
	private static final String key_jdk_bin="jdk.bin";
	private static final String key_spark_home="spark.home";
	private static final String key_history_server="spark.history.server";
	private static final String key_launch_mode="spark.launch.mode";
	
	private String tmpFolder;
	private String srcFolder="src";
	private String classesFolder="classes";
	private String targetFolder="target";
	private String jdkBin;
	private String sparkHome;
	private String sparkHistoryServer;
	private String sparkLaunchMode;
	private FlowServerConf oozieServerConf;
	
	public SparkServerConf(String confFile){
		PropertiesConfiguration pc = PropertiesUtil.getPropertiesConfig(confFile);
		tmpFolder = pc.getString(key_tmp_folder);
		jdkBin = pc.getString(key_jdk_bin);
		sparkHome = pc.getString(key_spark_home);
		sparkHistoryServer = pc.getString(key_history_server);
		sparkLaunchMode = pc.getString(key_launch_mode, "oozie");
		oozieServerConf = new OozieConf(confFile);
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

	public String getSparkHome() {
		return sparkHome;
	}

	public void setSparkHome(String sparkHome) {
		this.sparkHome = sparkHome;
	}

	public String getSparkHistoryServer() {
		return sparkHistoryServer;
	}

	public void setSparkHistoryServer(String sparkHistoryServer) {
		this.sparkHistoryServer = sparkHistoryServer;
	}

	public String getSparkLaunchMode() {
		return sparkLaunchMode;
	}

	public void setSparkLaunchMode(String sparkLaunchMode) {
		this.sparkLaunchMode = sparkLaunchMode;
	}

	public FlowServerConf getOozieServerConf() {
		return oozieServerConf;
	}

	public void setOozieServerConf(FlowServerConf oozieServerConf) {
		this.oozieServerConf = oozieServerConf;
	}
}
