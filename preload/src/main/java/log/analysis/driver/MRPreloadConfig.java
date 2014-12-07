package log.analysis.driver;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.cld.util.StringUtil;

public class MRPreloadConfig {
	public static final Logger logger = Logger.getLogger(MRPreloadConfig.class);
	
	public static final String LOCAL_WORKFOLDER="local.work.folder";
	public static final String HDFS_WORKFOLDER="hdfs.work.folder";
	public static final String RAWINPUT_FOLDER="rawinput.folder";
	public static final String RAWINPUT_INSPECT_INTERVAL="rawinput.inspect.interval";
	public static final String RAWINPUT_OUTPUT_THRESHOLD="rawinput.output.threshold";
	public static final String RAWINPUT_OUTPUTFOLDER="rawinput.output.folder";
	public static final String PROCESSING_FOLDER="processing.folder";
	public static final String SUC_FOLDER="suc.folder";
	public static final String ERROR_FOLDER="err.folder";
	public static final String HADOOP_EXE="hadoop.exe";
	public static final String REDUCE_TASKS="reduce.tasks";
	public static final String REDUCE_MEOMRY="reduce.memory";
	
	public static final String MRPROCESS_JAR="mrprocess.jar";
	public static final String REDUCE_COMMAND="reduce.command";
	
	private String localWorkFolder;
	private String hdfsWorkFolder;
	private String rawInputFolder ="rawinput";
	private int rawInputInspectInterval = 300; //unit of second
	private int rawInputOutputThreshold = -1; //number of input files
	private String rawInputOutputFolder = "seedinput";
	private String processingFolder = "processing";
	private String sucFolder = "success";
	private String errFolder = "error";
	private String hadoopExe;
	private int reduceTasks=3;
	private int reduceMemory=1024;//in MB
	
	private String mrprocessJar;
	private String reduceCommand;

	public MRPreloadConfig(String fileName){
		PropertiesConfiguration pc = null;
		Map<String, Object> params = new HashMap<String, Object>();
		try{
			pc =new PropertiesConfiguration(fileName);
		}catch(Exception e){
			logger.error("", e);
		}
		
		String strVal = pc.getString(LOCAL_WORKFOLDER);
		if (strVal!=null){
			localWorkFolder=strVal;
			params.put(LOCAL_WORKFOLDER, localWorkFolder);
		}else{
			logger.error(LOCAL_WORKFOLDER + " not configured.");
		}
		
		strVal = pc.getString(HDFS_WORKFOLDER);
		if (strVal!=null){
			hdfsWorkFolder=strVal;
			params.put(HDFS_WORKFOLDER, hdfsWorkFolder);
		}else{
			logger.error(HDFS_WORKFOLDER + " not configured.");
		}
		
		strVal = pc.getString(RAWINPUT_FOLDER);
		if (strVal!=null){
			rawInputFolder = strVal;
			params.put(RAWINPUT_FOLDER, rawInputFolder);
		}
		
		strVal = pc.getString(RAWINPUT_INSPECT_INTERVAL);
		if (strVal!=null){
			rawInputInspectInterval = Integer.parseInt(strVal);
		}
		
		strVal = pc.getString(RAWINPUT_OUTPUT_THRESHOLD);
		if (strVal!=null){
			rawInputOutputThreshold = Integer.parseInt(strVal);
		}
		
		strVal = pc.getString(RAWINPUT_OUTPUTFOLDER);
		if (strVal!=null){
			rawInputOutputFolder = strVal;
			params.put(RAWINPUT_OUTPUTFOLDER, rawInputOutputFolder);
		}
		
		strVal = pc.getString(PROCESSING_FOLDER);
		if (strVal!=null){
			processingFolder = strVal;
			params.put(PROCESSING_FOLDER, processingFolder);
		}
		
		strVal = pc.getString(HADOOP_EXE);
		if (strVal!=null){
			setHadoopExe(strVal);
		}
		
		strVal = pc.getString(REDUCE_TASKS);
		if (strVal!=null){
			setReduceTasks(Integer.parseInt(strVal));
		}
		
		strVal = pc.getString(REDUCE_MEOMRY);
		if (strVal!=null){
			setReduceMemory(Integer.parseInt(strVal));
		}
		
		strVal = pc.getString(MRPROCESS_JAR);
		if (strVal!=null){
			setMrprocessJar(strVal);
		}else{
			logger.error("mr process jar is not specified.");
		}
		
		strVal = pc.getString(REDUCE_COMMAND);
		if (strVal!=null){
			reduceCommand=strVal;
			reduceCommand = StringUtil.fillParams(reduceCommand, params, "$", "");
		}else{
			logger.error("mr command is null.");
		}
		
		strVal = pc.getString(SUC_FOLDER);
		if (strVal!=null){
			setSucFolder(strVal);
			params.put(SUC_FOLDER, strVal);
		}
		
		strVal = pc.getString(ERROR_FOLDER);
		if (strVal!=null){
			setErrFolder(strVal);
			params.put(ERROR_FOLDER, strVal);
		}
	}

	public String getLocalWorkFolder() {
		return localWorkFolder;
	}

	public void setLocalWorkFolder(String localWorkFolder) {
		this.localWorkFolder = localWorkFolder;
	}

	public String getHdfsWorkFolder() {
		return hdfsWorkFolder;
	}

	public void setHdfsWorkFolder(String hdfsWorkFolder) {
		this.hdfsWorkFolder = hdfsWorkFolder;
	}

	public String getRawInputFolder() {
		return rawInputFolder;
	}

	public void setRawInputFolder(String rawInputFolder) {
		this.rawInputFolder = rawInputFolder;
	}

	public int getRawInputInspectInterval() {
		return rawInputInspectInterval;
	}

	public void setRawInputInspectInterval(int rawInputInspectInterval) {
		this.rawInputInspectInterval = rawInputInspectInterval;
	}

	public String getRawInputOutputFolder() {
		return rawInputOutputFolder;
	}

	public void setRawInputOutputFolder(String rawInputOutputFolder) {
		this.rawInputOutputFolder = rawInputOutputFolder;
	}

	public String getProcessingFolder() {
		return processingFolder;
	}

	public void setProcessingFolder(String processingFolder) {
		this.processingFolder = processingFolder;
	}
	
	public String getMrCommand() {
		return reduceCommand;
	}

	public void setMrCommand(String mrCommand) {
		this.reduceCommand = mrCommand;
	}

	public int getRawInputOutputThreshold() {
		return rawInputOutputThreshold;
	}

	public void setRawInputOutputThreshold(int rawInputOutputThreshold) {
		this.rawInputOutputThreshold = rawInputOutputThreshold;
	}

	public String getMrprocessJar() {
		return mrprocessJar;
	}

	public void setMrprocessJar(String mrprocessJar) {
		this.mrprocessJar = mrprocessJar;
	}

	public String getSucFolder() {
		return sucFolder;
	}

	public void setSucFolder(String sucFolder) {
		this.sucFolder = sucFolder;
	}

	public String getErrFolder() {
		return errFolder;
	}

	public void setErrFolder(String errFolder) {
		this.errFolder = errFolder;
	}

	public int getReduceTasks() {
		return reduceTasks;
	}

	public void setReduceTasks(int reduceTasks) {
		this.reduceTasks = reduceTasks;
	}

	public String getHadoopExe() {
		return hadoopExe;
	}

	public void setHadoopExe(String hadoopExe) {
		this.hadoopExe = hadoopExe;
	}

	public int getReduceMemory() {
		return reduceMemory;
	}

	public void setReduceMemory(int reduceMemory) {
		this.reduceMemory = reduceMemory;
	}

}
