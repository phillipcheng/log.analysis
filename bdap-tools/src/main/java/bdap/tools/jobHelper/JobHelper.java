package bdap.tools.jobHelper;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.TimeZone;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.MapContext;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


public class JobHelper {
	
	private static final Logger logger = LoggerFactory.getLogger(JobHelper.class);
	
	private FileSystem hdfs;
	private org.apache.commons.configuration.Configuration config;
	private OozieClient oozieClient;
	private ResourceBundle resourceBundle = ResourceBundle.getBundle("bdap.tools.jobHelper.message", Locale.getDefault()); 
	private static final JexlEngine jexl = new JexlBuilder().cache(512).strict(true).silent(false).create();
	
	private Map<String, PropertiesConfiguration> jobsProperties;
	
	private Options options;
	private CommandLine commandLine;
	private SimpleDateFormat sdf;
	
	//Configuration file configuration item
	public static final String CFG_SCAN_JOB_FILTER="scan.job.filter";
	public static final String CFG_WORKFLOW_ID="wfid";
	public static final String CFG_MAPREDUCE_OUTPUT_FILEOUTPUTFORMAT_OUTPUTDIR="mapreduce.output.fileoutputformat.outputdir";
	public static final String CFG_OOZIE_WF_APPLICATION_PATH="oozie.wf.application.path";
	public static final String CFG_OOZIE_WF_RERUN_SKIP_NODES="oozie.wf.rerun.skip.nodes";
	public static final String CFG_MAX_CONCURRENT_JOBS="max.concurrent.jobs";
	public static final String CFG_OOZIE_USER_NAME="oozie.user.name";
	
	//for job.<AppName>. only
	public static final String CFG_SCAN_NODES="scan.nodes";
	public static final String CFG_SCAN_PATHS="scan.paths";	
	
	private String scanJobFilter;
	private int maxConcurrentJobs;
	
	public static Set<String> rerunableStatus;
	{
		rerunableStatus=new HashSet<String>();
		rerunableStatus.add("SUCCEEDED");
		rerunableStatus.add("KILLED");
		rerunableStatus.add("FAILED");
	}
	
	public static void main(String[] args) throws Exception{
		JobHelper jobHelper=new JobHelper();
		jobHelper.execute(args);
	}
	
	public void execute(String[] args){
		try {
			init(args);
			if(commandLine.hasOption("scan")){
				scan();
			}
			if(commandLine.hasOption("rerun")){
				rerun();
			}
		} catch (Exception e) {
			logger.error(String.format("%s failed!", this.getClass().getName()),e);
		}		
	}
	
	private void rerun() {		
		String[] rerunParams=commandLine.getOptionValues("rerun");
		String rerunJobFile=rerunParams[0];
		String rerunResultFile=rerunParams[1];
		
		logger.info("Start to rerun job, rerun job list:{}, rerun result output:{}", rerunJobFile, rerunResultFile);
		List<String> jobInfoList=null;
		FileOutputStream fos=null;
		try {
			jobInfoList=FileUtils.readLines(new File(rerunJobFile));
			fos=FileUtils.openOutputStream(new File(rerunResultFile));
		} catch (IOException e) {
			logger.error("Failed to access rerun job files.",e);
			System.exit(1);
		}
		
		if(jobInfoList==null) return;
		
		int jobIdx=0;
		for(String jobInfo:jobInfoList){
			jobIdx++;
			String[] jobInfoItems = jobInfo.split(",", -1);
			if(jobInfoItems==null || jobInfoItems.length==0) continue;
			String jobId=jobInfoItems[0];
			logger.info("Start to rerun job({}/{}): {}", jobIdx++,jobInfoList.size(),jobId);
			try{
				WorkflowJob job = oozieClient.getJobInfo(jobId);
				if(job==null){
					logger.info("Job doesn't exist, skip!", jobId);
					IOUtils.write(String.format("%s,SKIP_NOT_EXIST\n",jobId), fos);
					continue;
				}
				String jobStatus=job.getStatus().toString();
				if(!rerunableStatus.contains(jobStatus)){
					logger.info("Job status is {}, skip. It must be SUCCEEDED,FAILED,KILLED.");
					IOUtils.write(String.format("%s,SKIP_INCORRECT_STATUS\n",jobId), fos);
					continue;
				}
				Properties jobReadConf = loadXMLConfiguration(job.getConf());
//				String wfApplicationPath=jobReadConf.getProperty(CFG_OOZIE_WF_APPLICATION_PATH);
//				if(wfApplicationPath==null || wfApplicationPath.isEmpty()){
//					logger.info("Get empty oozie.wf.application.path, skip.");
//					IOUtils.write(String.format("%s,SKIP_ERROR_WF_APP_PATH\n",jobId), fos);
//					continue;
//				}
				
				PropertiesConfiguration jobProperties = jobsProperties.get(job.getAppName());
				String wfRerunSkipNodes=jobProperties.getString(CFG_OOZIE_WF_RERUN_SKIP_NODES);
				if(wfRerunSkipNodes==null || wfRerunSkipNodes.isEmpty()){
					logger.info("Recently rerun skip nodes must specified.");
					IOUtils.write(String.format("%s,SKIP_ERROR_SKIP_NODES_MANDATORY\n",jobId), fos);
					continue;
				}
				
				Properties jobRerunConf=new Properties();
				jobRerunConf.putAll(jobReadConf);
				jobRerunConf.setProperty(CFG_OOZIE_WF_RERUN_SKIP_NODES, wfRerunSkipNodes);
				
							
				checkMaximumConcurrentJob();
				oozieClient.reRun(jobId, jobRerunConf);
				WorkflowJob submitJob = checkJobStatus(jobId);
				logger.info("Complete rerun with status:{}.", submitJob.getStatus().toString());
				IOUtils.write(String.format("%s,%s\n",jobId,submitJob.getStatus().toString()), fos);
				
			}catch (Exception e) {
				logger.warn(String.format("Failed to rerun job:%s}",jobId), e);
				try {
					IOUtils.write(String.format("%s,SKIP_ERROR_UNKNOWN\n",jobId), fos);
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}
		
		IOUtils.closeQuietly(fos);
		
	}
	
	private void checkMaximumConcurrentJob(){
		while(true){
			try {
				List<WorkflowJob> jobs = oozieClient.getJobsInfo("status=PREP;status=RUNNING", 1, this.maxConcurrentJobs+1);
				if(jobs!=null & jobs.size()<this.maxConcurrentJobs) break;
				logger.info("Waiting other job complete. Jobs:{}/{}", jobs.size(), this.maxConcurrentJobs);
				Thread.sleep(10000L);
			} catch (Exception e) {
				logger.warn("Retry check max concurrent job.", e);
				try {
					Thread.sleep(10000L);
				} catch (InterruptedException e1) {
				}
			}
		}	
	}
	
	private WorkflowJob checkJobStatus(String jobId){
		while(true){
			try {
				WorkflowJob job = oozieClient.getJobInfo(jobId);
				if(job!=null){
					String jobStatus=job.getStatus().toString();
					if(!"RUNNING".equalsIgnoreCase(jobStatus) && !"PREP".equalsIgnoreCase(jobStatus)){
						return job;
					}
				}
				logger.info("Waiting job:{} complete.", jobId);
				Thread.sleep(10000L);
			} catch (Exception e) {
				logger.warn("Retry check job status.", e);
				try {
					Thread.sleep(10000L);
				} catch (InterruptedException e1) {
				}
			}
		}	
	}
	
	private void scan() {
		String scanOutputFile=commandLine.getOptionValue("scan");
		
		logger.info("Start to scan jobs with filter:{}, the scan result output to:{}", scanJobFilter, scanOutputFile);
		
		FileOutputStream fos=null;
		try {
			fos=FileUtils.openOutputStream(new File(scanOutputFile));
		} catch (IOException e) {
			logger.error(String.format("Cannot create scan output file"),e);
			System.exit(1);
		}
		int start = 1;
		int len = 50;
		List<WorkflowJob> jobs = null;
		Set<String> jobIdSet = new HashSet<String>();
		do {
			try {
				jobs = oozieClient.getJobsInfo(scanJobFilter, start, len);
				for (WorkflowJob job : jobs) {
					jobIdSet.add(job.getId());
				}
			} catch (OozieClientException e) {
				logger.warn("Query job list failed start:{}, len:{}", start, len);
			} finally {
				start = start + len;
			}
		} while (jobs != null && jobs.size() == len);
		
		logger.info("Total jobs:{}", jobIdSet.size());
		int jobIdx=1;
		for (String jobId : jobIdSet) {
			logger.info("Start to scan job({}/{}): {}", jobIdx++,jobIdSet.size(),jobId);
			try {
				WorkflowJob job = oozieClient.getJobInfo(jobId);

				String appName = job.getAppName();
				PropertiesConfiguration jobProperties = jobsProperties.get(appName);
				if (jobProperties == null)
					continue;

				// Calculate scan paths
				String[] scanPathsExp = jobProperties.getStringArray(CFG_SCAN_PATHS);
				List<String> scanPaths = new ArrayList<String>();
				if (scanPathsExp != null && scanPathsExp.length > 0) {
					for (String pathExp : scanPathsExp) {
						JexlExpression exp = jexl.createExpression(pathExp);
						Map<String, Object> vars = new HashMap<String, Object>();
						vars.put(CFG_WORKFLOW_ID, job.getId());
						JexlContext jexlCtx = new MapContext(vars);
						String path = (String) exp.evaluate(jexlCtx);
						scanPaths.add(path);
					}
				}

				// Scan node
				String[] scanNodes = jobProperties.getStringArray(CFG_SCAN_NODES);
				Set<String> scanNodesSet=new HashSet<String>();
				scanNodesSet.addAll(Arrays.asList(scanNodes));
				List<String> nodeStatusList = new ArrayList<String>();
				
				List<WorkflowAction> actions = job.getActions();
				if (actions != null && actions.size() > 0) {
					for (WorkflowAction action : actions) {
						nodeStatusList.add(String.format("%s=%s", action.getName(),action.getStatus().toString()));
						
						//add the output path to scan path
						if(scanNodesSet.contains(action.getName())){
							Properties actionProperties = loadXMLConfiguration(action.getConf());
							String outputDir = actionProperties.getProperty(CFG_MAPREDUCE_OUTPUT_FILEOUTPUTFORMAT_OUTPUTDIR);
							scanPaths.add(outputDir);
						}
					}
				}

				// scan paths
				List<String> scanPathResult = new ArrayList<String>();
				int totalFileCount=0;
				long totalFileSize = 0;				
				for (String pathStr : scanPaths) {
					Path path = new Path(pathStr);
					if (hdfs.exists(path)) {
						FileStatus fileStatus = hdfs.getFileStatus(path);
						Tuple2<Integer, Long> result = getNumberAndSizeOfInputFiles(fileStatus, hdfs);
						int pathFileCount=result._1;
						long pathFileSize=result._2;
						totalFileCount+=pathFileCount;
						totalFileSize+=pathFileSize;
						scanPathResult.add(String.format("%s=%s(%s)", pathStr, pathFileCount,pathFileSize));
					} else {
						scanPathResult.add(String.format("%s=0(0)", pathStr));
					}
				}
//				long totalFiles = 0;
//				for (String pathStr : scanPaths) {
//					Path path = new Path(pathStr);
//					if (hdfs.exists(path)) {
//						if (hdfs.isDirectory(path)) {
//							RemoteIterator<LocatedFileStatus> files = hdfs.listFiles(path, true);
//							long pathFiles = 0;
//							while (files.hasNext()) {
//								LocatedFileStatus fileStatus = files.next();
//								if(fileStatus.isDirectory()){
//									long filesCount=getNumberOfInputFiles(fileStatus, hdfs);
//									pathFiles=pathFiles+filesCount;
//								}else{
//									if(!"_SUCCESS".equalsIgnoreCase(fileStatus.getPath().getName())){
//										totalFiles++;
//										pathFiles++;
//									}
//								}
//							}
//							scanPathResult.add(String.format("%s=%s", pathStr, pathFiles));
//						} else {
//							scanPathResult.add(String.format("%s=1", pathStr));
//							totalFiles++;
//						}
//					} else {
//						scanPathResult.add(String.format("%s=0", pathStr));
//					}
//				}

				// jobid, appName, job created time, jobStatus, totalFiles, actionStatus, pathFileCount
//				String info = String.format("%s,%s,%s,%s,%s,%s,%s\r\n", job.getId(), appName, sdf.format(job.getCreatedTime()),
//						job.getStatus(), totalFiles, String.join("|", nodeStatusList),String.join("|", scanPathResult));
				
				
				
				// jobid, appName, job created time, jobStatus, totalFileCount,totalFileSize, actionStatus, pathFileCount				
				String info = String.format("%s,%s,%s,%s,%s,%s,%s,%s\r\n", job.getId(), appName, sdf.format(job.getCreatedTime()),
						job.getStatus(), totalFileCount, totalFileSize, String.join("|", nodeStatusList),String.join("|", scanPathResult));
				IOUtils.write(info, fos);
			} catch (Exception e) {				
				logger.warn("Failed to scan job:{}", jobId);
			}
		}
		
		IOUtils.closeQuietly(fos);
	}
	
	private Tuple2<Integer,Long> getNumberAndSizeOfInputFiles(FileStatus status, FileSystem fs) throws IOException  {
	    int inputFileCount=0;
	    long inputFileSize=0;
	    if(status.isDirectory()) {
	        FileStatus[] files = fs.listStatus(status.getPath());
	        for(FileStatus file: files) {
	        	Tuple2<Integer,Long> result=getNumberAndSizeOfInputFiles(file, fs);
	        	inputFileCount+=result._1;
	        	inputFileSize +=result._2;
	        }
	    } else {
	        inputFileCount=1;
	        inputFileSize=status.getLen();
	    }

	    return new Tuple2<Integer,Long>(inputFileCount,inputFileSize);
	}
	
	public static class Tuple2<T1,T2>{
		public T1 _1;
		public T2 _2;
		
		public Tuple2(T1 _1, T2 _2) {
			this._1=_1;
			this._2=_2;
		}
	}
	
	/**
	 * Recursively determines number of input files in an HDFS directory
	 *
	 * @param status instance of FileStatus
	 * @param fs instance of FileSystem
	 * @return number of input files within particular HDFS directory
	 * @throws IOException
	 */
	private int getNumberOfInputFiles(FileStatus status, FileSystem fs) throws IOException  {
	    int inputFileCount = 0;
	    if(status.isDirectory()) {
	        FileStatus[] files = fs.listStatus(status.getPath());
	        for(FileStatus file: files) {
	            inputFileCount += getNumberOfInputFiles(file, fs);
	        }
	    } else {
	        inputFileCount ++;
	    }

	    return inputFileCount;
	}
	
	/**
	 * Recursively determines size of input files in an HDFS directory
	 *
	 * @param status instance of FileStatus
	 * @param fs instance of FileSystem
	 * @return size of input files within particular HDFS directory
	 * @throws IOException
	 */
	private long getSizeOfInputFiles(FileStatus status, FileSystem fs) throws IOException  {
	    long size = 0;
	    if(status.isDirectory()) {
	        FileStatus[] files = fs.listStatus(status.getPath());
	        for(FileStatus file: files) {
	            size += getSizeOfInputFiles(file, fs);
	        }
	    } else {
	    	size=status.getLen();
	    }

	    return size;
	}

	private void init(String[] args) throws Exception {
		sdf=new SimpleDateFormat("YYYY-MM-dd hh:mm:ss.SSSS");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));				
				
		options = new Options();		
		Option configOption = OptionBuilder.withArgName( "file" ).hasArg().withDescription(resourceBundle.getString("cli_config_description")).create( "config" );
		options.addOption(configOption);
		Option scanOption = OptionBuilder.withArgName( "file" ).hasArg().withDescription(resourceBundle.getString("cli_scan_description")).create( "scan" );
		options.addOption(scanOption);
		options.addOption(Option.builder("rerun").hasArg().numberOfArgs(2).argName("file").desc(resourceBundle.getString("cli_rerun_description")).build());
		options.addOption("help", false, resourceBundle.getString("cli_help_description"));
		
		CommandLineParser parser = new DefaultParser(); 
		commandLine=parser.parse(options, args);
		
		if(commandLine.hasOption("help")){
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("JobHelper", options);
		}
		
		if(commandLine.hasOption("config")){		
			URL configFileURL = new URL(commandLine.getOptionValue("config"));
			String extension=FilenameUtils.getExtension(configFileURL.getFile());
			if("properties".equalsIgnoreCase(extension)){
				config=new PropertiesConfiguration(configFileURL);
			}else if("xml".equalsIgnoreCase(extension)){
				config=new XMLConfiguration(configFileURL);
			}else{
				logger.error("Unknown config file type, it should be properties or xml");
				System.exit(1);
			}		
		}else{
			logger.error("Please specify config file, it is mandatory!");
			System.exit(1);
		}
		
		this.scanJobFilter=config.getString(CFG_SCAN_JOB_FILTER, "status=KILLED;status=FAILED");
		this.maxConcurrentJobs=config.getInt(CFG_MAX_CONCURRENT_JOBS,5);
		
		//read job configuration
		readJobProperties();
		
		//initial oozie client
		if(config.getString("oozieUrl")!=null){
			oozieClient=new OozieClient(config.getString("oozieUrl"));
			logger.info("Initial oozie client successful");
		}
		
		//initial HDFS
		if(config.containsKey("hdfs.user") && config.containsKey("fs.defaultFS")){
			UserGroupInformation ugi;
			ugi = UserGroupInformation.createProxyUser(config.getString("hdfs.user"), UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					Configuration hdfsConfig=new Configuration();
					hdfsConfig.set("fs.defaultFS", config.getString("fs.defaultFS"));
					hdfs=FileSystem.get(hdfsConfig);
					logger.info("Initial HDFS client successful");
					return null;
				}
			});
		}
	}

	private void readJobProperties() {
		jobsProperties=new HashMap<String, PropertiesConfiguration>();		
		Iterator<String> keyIterator=config.getKeys();		
		while(keyIterator.hasNext()){
			String key=keyIterator.next();
			if(key.startsWith("job.")){
				String name=StringUtils.substringBetween(key, "job.", ".");				
				String propertyName=StringUtils.substringAfter(key, "job."+name+".");
				PropertiesConfiguration jobProperties=jobsProperties.get(name);
				if(jobProperties==null){
					jobProperties=new PropertiesConfiguration();
					jobsProperties.put(name, jobProperties);
				}
				jobProperties.setProperty(propertyName, config.getProperty(key));
			}
		}
	}
	
	/**
	 * Used to parse the configuration with the format:
	 * <configuration>
	 *   <property>
	 *       <name>oozie.libpath</name>
	 *       <value>hdfs://192.85.247.104:19000/bdap-VVERSIONN/engine/lib</value>
	 *    </property>
	 * </configuration>
	 * @param conf  xml text
	 * @return
	 * @throws Exception
	 */
	private Properties loadXMLConfiguration(String conf) throws Exception{
		Properties properties=new Properties();
		DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		ByteArrayInputStream bais=new ByteArrayInputStream(conf.getBytes("UTF-8"));
		Document doc = documentBuilder.parse(bais);
		
		Node configuration = doc.getElementsByTagName("configuration").item(0);
		Element configurationElement=(Element)configuration;
		NodeList propertyNodeList=configurationElement.getElementsByTagName("property");
		if(propertyNodeList!=null && propertyNodeList.getLength()>0){
			for(int i=0;i<propertyNodeList.getLength();i++){
				Node propertyNode=propertyNodeList.item(i);
//				if(propertyNode.getNodeType()!=Node.ELEMENT_NODE) continue;
				Element propertyElement = (Element) propertyNode;
				String name=propertyElement.getElementsByTagName("name").item(0).getTextContent();
				String value=propertyElement.getElementsByTagName("value").item(0).getTextContent();
				properties.setProperty(name, value);
			}
		}
		return properties;
	}
}
