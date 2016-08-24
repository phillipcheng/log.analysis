package etl.cmd.test;

import java.io.File;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.junit.Before;

import etl.engine.InvokeMapper;
import etl.util.FilenameInputFormat;
import etl.util.Util;

public class TestETLCmd {
	public static final Logger logger = Logger.getLogger(TestETLCmd.class);
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	
	public static final String remoteUser = "dbadmin";
	
	private static String cfgProperties="testETLCmd.properties";
	//private static String cfgProperties="testETLCmd_192.85.247.104.properties";
	
	private static String key_localFolder="localFolder";
	private static String key_projectFolder="projectFolder";
	private static String key_defaultFs="defaultFS";
	private static String key_jobTracker="jobTracker";
	private static String key_test_sftp="test.sftp";
	private static String key_test_kafka="test.kafka";
	
	
	private PropertiesConfiguration pc;
	
	private String localFolder = "";
	private String projectFolder = "";
	private FileSystem fs;
	private String defaultFS;
	private Configuration conf;
	private boolean testSftp=true;
	private boolean testKafka=true;
	
	//
	public static void setCfgProperties(String testProperties){
		cfgProperties = testProperties;
	}
	
	@Before
    public void setUp() {
		try{
			pc = Util.getPropertiesConfig(cfgProperties);
			localFolder = pc.getString(key_localFolder);
			projectFolder = pc.getString(key_projectFolder);
			setTestSftp(pc.getBoolean(key_test_sftp, true));
			setTestKafka(pc.getBoolean(key_test_kafka, true));
			conf = new Configuration();
			String jobTracker=pc.getString(key_jobTracker);
			if (jobTracker!=null){
				String host = jobTracker.substring(0,jobTracker.indexOf(":"));
				conf.set("mapreduce.jobtracker.address", jobTracker);
				conf.set("yarn.resourcemanager.hostname", host);
				conf.set("mapreduce.framework.name", "yarn");
				conf.set("yarn.nodemanager.aux-services", "mapreduce_shuffle");
			}
			defaultFS = pc.getString(key_defaultFs);
			conf.set("fs.defaultFS", defaultFS);
			if (defaultFS.contains("127.0.0.1")){
				fs = FileSystem.get(conf);
			}else{
				UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
				ugi.doAs(new PrivilegedExceptionAction<Void>() {
					public Void run() throws Exception {
						fs = FileSystem.get(conf);
						return null;
					}
				});
			}
		}catch(Exception e){
			logger.error("", e);
		}
    }
	
	public List<String> mapTest(String remoteCfgFolder, String remoteInputFolder, String remoteOutputFolder,
			String staticProperties, String[] inputDataFiles, String cmdClassName, boolean useFileNames) throws Exception {
		try {
			getFs().delete(new Path(remoteCfgFolder), true);
			getFs().delete(new Path(remoteInputFolder), true);
			getFs().delete(new Path(remoteOutputFolder), true);
			getFs().mkdirs(new Path(remoteCfgFolder));
			getFs().mkdirs(new Path(remoteInputFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + staticProperties), new Path(remoteCfgFolder + staticProperties));
			for (String csvFile : inputDataFiles) {
				getFs().copyFromLocalFile(new Path(getLocalFolder() + csvFile), new Path(remoteInputFolder + csvFile));
			}
			// run job
			getConf().set(InvokeMapper.cfgkey_cmdclassname, cmdClassName);
			getConf().set(InvokeMapper.cfgkey_wfid, sdf.format(new Date()));
			getConf().set(InvokeMapper.cfgkey_staticconfigfile, remoteCfgFolder + staticProperties);
			Job job = Job.getInstance(getConf(), "testCmd");
			job.setMapperClass(etl.engine.InvokeMapper.class);
			job.setNumReduceTasks(0);// no reducer
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			if (useFileNames){
				job.setInputFormatClass(FilenameInputFormat.class);
			}else{
				FileInputFormat.setInputDirRecursive(job, true);
			}
			FileInputFormat.setInputDirRecursive(job, true);
			FileInputFormat.addInputPath(job, new Path(remoteInputFolder));
			FileOutputFormat.setOutputPath(job, new Path(remoteOutputFolder));
			job.waitForCompletion(true);

			// assertion
			List<String> output = Util.getMROutput(getFs(), remoteOutputFolder);
			return output;
		} catch (Exception e) {
			logger.error("", e);
		}
		return null;
	}
	
	/**
	 * 
	 * @param remoteCfgFolder
	 * @param remoteInputFolder
	 * @param remoteOutputFolder
	 * @param staticProperties
	 * @param inputDataFiles
	 * @param cmdClassName
	 * @param useFileNames: 
	 * @return
	 * @throws Exception
	 */
	public List<String> mrTest(String remoteCfgFolder, String remoteInputFolder, String remoteOutputFolder,
			String staticProperties, String[] inputDataFiles, String cmdClassName, boolean useFileNames, boolean nullable) throws Exception {
		try {
			getFs().delete(new Path(remoteCfgFolder), true);
			getFs().delete(new Path(remoteInputFolder), true);
			getFs().delete(new Path(remoteOutputFolder), true);
			getFs().mkdirs(new Path(remoteCfgFolder));
			getFs().mkdirs(new Path(remoteInputFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + staticProperties), new Path(remoteCfgFolder + staticProperties));
			for (String csvFile : inputDataFiles) {
				getFs().copyFromLocalFile(new Path(getLocalFolder() + csvFile), new Path(remoteInputFolder + csvFile));
			}
			//run job
			getConf().set(InvokeMapper.cfgkey_cmdclassname, cmdClassName);
			getConf().set(InvokeMapper.cfgkey_wfid, sdf.format(new Date()));
			getConf().set(InvokeMapper.cfgkey_staticconfigfile, remoteCfgFolder + staticProperties);
			getConf().set("mapreduce.output.textoutputformat.separator", ",");
			Job job = Job.getInstance(getConf(), "testCmd");
			job.setMapperClass(etl.engine.InvokeReducerMapper.class);
			if (nullable){
				job.setReducerClass(etl.engine.InvokeNullableReducer.class);
			}else{
				job.setReducerClass(etl.engine.InvokeReducer.class);
			}
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			if (useFileNames){
				job.setInputFormatClass(FilenameInputFormat.class);
			}else{
				FileInputFormat.setInputDirRecursive(job, true);
			}
			FileInputFormat.addInputPath(job, new Path(remoteInputFolder));
			FileOutputFormat.setOutputPath(job, new Path(remoteOutputFolder));
			job.waitForCompletion(true);

			// assertion
			List<String> output = Util.getMROutput(getFs(), remoteOutputFolder);
			return output;
		} catch (Exception e) {
			logger.error("", e);
		}
		return null;
	}
	
	public void setupWorkflow(String remoteLibFolder, String remoteCfgFolder, String localTargetFolder, String libName, 
			String localLibFolder, String verticaLibName) throws Exception{
    	Path remoteLibPath = new Path(remoteLibFolder);
    	if (fs.exists(remoteLibPath)){
    		fs.delete(remoteLibPath, true);
    	}
    	//copy workflow to remote
    	String workflow = getLocalFolder() + File.separator + "workflow.xml";
		String remoteWorkflow = remoteLibFolder + File.separator + "workflow.xml";
		fs.copyFromLocalFile(new Path(workflow), new Path(remoteWorkflow));
		//copy job properties to remote
		String jobProperties = getLocalFolder() + File.separator + "job.properties";
		String remoteJobProperties = remoteLibFolder + File.separator + "job.properties";
		fs.copyFromLocalFile(new Path(jobProperties), new Path(remoteJobProperties));
		fs.copyFromLocalFile(new Path(localTargetFolder + libName), new Path(remoteLibFolder + "/lib/" +libName));
		fs.copyFromLocalFile(new Path(localLibFolder + verticaLibName), new Path(remoteLibFolder+ "/lib/" + verticaLibName));
		
		//copy etlcfg
		Path remoteCfgPath = new Path(remoteCfgFolder);
		if (fs.exists(remoteCfgPath)){
			fs.delete(new Path(remoteCfgFolder), true);
		}
		File localDir = new File(getLocalFolder());
		String[] cfgs = localDir.list();
		for (String cfg:cfgs){
			String lcfg = getLocalFolder() + File.separator + cfg;
			String rcfg = remoteCfgFolder + "/" + cfg;
			fs.copyFromLocalFile(new Path(lcfg), new Path(rcfg));
		}
	}
	
	public void copyWorkflow(String remoteLibFolder, String[] workflows) throws Exception{
    	for (String wf: workflows){
    		//copy workflow to remote
    		String workflow = getLocalFolder() + File.separator + wf;
    		String remoteWorkflow = remoteLibFolder + File.separator + wf;
    		fs.copyFromLocalFile(new Path(workflow), new Path(remoteWorkflow));
    	}
	}

	//override by sub test cases
	public String getResourceSubFolder(){
		return "";
	}
	
	public String getLocalFolder() {
		return localFolder + getResourceSubFolder();
	}

	public FileSystem getFs() {
		return fs;
	}
	
	public String getDefaultFS() {
		return defaultFS;
	}
	
	public Configuration getConf(){
		return conf;
	}

	public boolean isTestKafka() {
		return testKafka;
	}

	public void setTestKafka(boolean testKafka) {
		this.testKafka = testKafka;
	}

	public boolean isTestSftp() {
		return testSftp;
	}

	public void setTestSftp(boolean testSftp) {
		this.testSftp = testSftp;
	}

	public String getProjectFolder() {
		return projectFolder;
	}

	public void setProjectFolder(String projectFolder) {
		this.projectFolder = projectFolder;
	}
}
