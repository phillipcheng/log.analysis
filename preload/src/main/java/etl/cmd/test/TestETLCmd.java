package etl.cmd.test;

import java.io.File;
import java.io.InputStream;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;

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
import etl.util.Util;

public class TestETLCmd {
	public static final Logger logger = Logger.getLogger(TestETLCmd.class);
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	
	public static final String remoteUser = "dbadmin";
	private static String cfgProperties="testETLCmd.properties";
	private static String key_localFolder="localFolder";
	private static String key_defaultFs="defaultFS";
	private static String key_jobTracker="jobTracker";
	
	private Properties p = new Properties();
	
	private String localFolder = "";
	private FileSystem fs;
	private String defaultFS;
	private Configuration conf;
	
	//
	public static void setCfgProperties(String testProperties){
		cfgProperties = testProperties;
	}
	
	@Before
    public void setUp() {
		try{
			InputStream input = this.getClass().getClassLoader().getResourceAsStream(cfgProperties);
			if (input!=null){
				p.load(input);
				localFolder = (p.getProperty(key_localFolder));
				conf = new Configuration();
				String jobTracker=p.getProperty(key_jobTracker);
				if (jobTracker!=null){
					String host = jobTracker.substring(0,jobTracker.indexOf(":"));
					conf.set("mapreduce.jobtracker.address", jobTracker);
					conf.set("yarn.resourcemanager.hostname", host);
					conf.set("mapreduce.framework.name", "yarn");
					conf.set("yarn.nodemanager.aux-services", "mapreduce_shuffle");
				}
				defaultFS = p.getProperty(key_defaultFs);
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
			}else{
				logger.error(String.format("%s not found in classpath.", cfgProperties));
			}
		}catch(Exception e){
			logger.error("", e);
		}
    }
	
	public List<String> mapTest(String remoteCfgFolder, String remoteInputFolder, String remoteOutputFolder,
			String staticProperties, String[] inputDataFiles, String cmdClassName) throws Exception {
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
	
	public List<String> mrTest(String remoteCfgFolder, String remoteInputFolder, String remoteOutputFolder,
			String staticProperties, String[] inputDataFiles, String cmdClassName) throws Exception {
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
			getConf().set("mapreduce.output.textoutputformat.separator", ",");
			Job job = Job.getInstance(getConf(), "testCmd");
			job.setMapperClass(etl.engine.InvokeReducerMapper.class);
			job.setReducerClass(etl.engine.InvokeReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
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
}
