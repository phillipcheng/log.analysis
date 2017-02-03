package etl.cmd.test;

import java.io.File;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;

import bdap.util.EngineConf;
import bdap.util.HdfsUtil;
import bdap.util.PropertiesUtil;
import bdaps.engine.core.ACmd;
import etl.engine.ETLCmd;
import etl.engine.InvokeMapper;
import etl.engine.types.InputFormatType;
import etl.input.FilenameInputFormat;
import etl.util.FieldType;
import etl.util.GlobExpPathFilter;
import scala.Function1;
import scala.Tuple2;

public abstract class TestETLCmd implements Serializable{
	private static final long serialVersionUID = 1L;
	public static final Logger logger = LogManager.getLogger(TestETLCmd.class);
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	
	private static String cfgProperties="testETLCmd.properties";
	//private static String cfgProperties="testETLCmd_192.85.247.104.properties";
	
	private static final String key_localFolder="localFolder";
	private static final String key_projectFolder="projectFolder";
	private static final String key_defaultFs="defaultFs";
	private static final String key_jobTracker="jobTracker";
	private static final String key_test_sftp="test.sftp";
	private static final String key_test_kafka="test.kafka";
	private static final String key_oozie_user="oozie.user";
	private static final String key_hdfs_user="hdfs.user";
	
	
	private transient PropertiesConfiguration pc;
	
	private String localFolder = "";
	private String projectFolder = "";
	private transient FileSystem fs;
	private String defaultFS;
	private transient Configuration conf;//v2
	private boolean testSftp=true;
	private boolean testKafka=true;
	private String oozieUser = "";
	
	//
	public static void setCfgProperties(String testProperties){
		cfgProperties = testProperties;
	}
	
	@Before
    public void setUp() throws Exception{
		pc = PropertiesUtil.getPropertiesConfig(cfgProperties);
		localFolder = pc.getString(key_localFolder);
		projectFolder = pc.getString(key_projectFolder);
		oozieUser = pc.getString(key_oozie_user);
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
			String hdfsUser = pc.getString(key_hdfs_user, "dbadmin");
			UserGroupInformation ugi = UserGroupInformation.createProxyUser(hdfsUser, UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					fs = FileSystem.get(conf);
					return null;
				}
			});
		}
    }
	
	//map test for cmd
	public List<String> mapTest(String remoteInputFolder, String remoteOutputFolder,
			String staticProperties, String[] inputDataFiles, String cmdClassName, Class<? extends InputFormat> inputFormatClass) throws Exception {
		return mapTest(remoteInputFolder, remoteOutputFolder, staticProperties, inputDataFiles, cmdClassName, inputFormatClass, null, false);
	}
	
	public List<String> mapTest(String remoteInputFolder, String remoteOutputFolder,
			String staticProperties, String[] inputDataFiles, String cmdClassName, boolean useFileNames) throws Exception {
		if (useFileNames)
			return mapTest(remoteInputFolder, remoteOutputFolder, staticProperties, inputDataFiles, cmdClassName, FilenameInputFormat.class);
		else
			return mapTest(remoteInputFolder, remoteOutputFolder, staticProperties, inputDataFiles, cmdClassName, NLineInputFormat.class);
	}
	
	public List<String> mapTest(String remoteInputFolder, String remoteOutputFolder,
			String staticProperties, String[] inputDataFiles, String cmdClassName, Class<? extends InputFormat> inputFormatClass, String inputFilter, boolean useIndividualFiles) throws Exception {
		try {
			getFs().delete(new Path(remoteOutputFolder), true);
			if (inputDataFiles != null && inputDataFiles.length > 0) {
				getFs().delete(new Path(remoteInputFolder), true);
				getFs().mkdirs(new Path(remoteInputFolder));
				for (String csvFile : inputDataFiles) {
					getFs().copyFromLocalFile(new Path(getLocalFolder() + csvFile), new Path(remoteInputFolder + csvFile));
				}
			} else if (!getFs().exists(new Path(remoteInputFolder))) {
				getFs().mkdirs(new Path(remoteInputFolder));
			}
			// run job
			getConf().set(EngineConf.cfgkey_cmdclassname, cmdClassName);
			getConf().set(EngineConf.cfgkey_wfid, sdf.format(new Date()));
			String cfgProperties = staticProperties;
			if (this.getResourceSubFolder()!=null){
				cfgProperties = this.getResourceSubFolder() + staticProperties;
			}
			getConf().set(EngineConf.cfgkey_staticconfigfile, cfgProperties);
			if (inputFilter!=null){
				getConf().set(GlobExpPathFilter.cfgkey_path_filters, inputFilter);
			}
			Job job = Job.getInstance(getConf(), "testCmd");
			job.setMapperClass(etl.engine.InvokeMapper.class);
			job.setNumReduceTasks(0);// no reducer
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			job.setInputFormatClass(inputFormatClass);
			if (inputFilter!=null){
				FileInputFormat.setInputPathFilter(job, GlobExpPathFilter.class);
			}
			FileInputFormat.setInputDirRecursive(job, true);
			if (!useIndividualFiles){
				FileInputFormat.addInputPath(job, new Path(remoteInputFolder));
			}else{
				FileInputFormat.addInputPath(job, new Path(remoteInputFolder+"*"));
			}
			FileOutputFormat.setOutputPath(job, new Path(remoteOutputFolder));
			job.waitForCompletion(true);

			// assertion
			List<String> output = HdfsUtil.stringsFromDfsFolder(getFs(), remoteOutputFolder);
			return output;
		} catch (Exception e) {
			logger.error("", e);
			return null;
		}
	}
	
	//map-reduce test for cmd
	public List<String> mrTest(List<Tuple2<String, String[]>> remoteFolderInputFiles, String remoteOutputFolder,
			String staticProperties, String cmdClassName, Class<? extends InputFormat> inputFormatClass,
			Class<? extends OutputFormat> outputFormatClass, int numReducer) throws Exception {
		getFs().delete(new Path(remoteOutputFolder), true);
		for (Tuple2<String, String[]> rfifs: remoteFolderInputFiles){
			if (rfifs._2.length > 0) {
				getFs().delete(new Path(rfifs._1), true);
				getFs().mkdirs(new Path(rfifs._1));
			}
			for (String csvFile : rfifs._2) {
				getFs().copyFromLocalFile(new Path(getLocalFolder() + csvFile), new Path(rfifs._1 + csvFile));
			}
		}
		
		//run job
		getConf().set(EngineConf.cfgkey_cmdclassname, cmdClassName);
		getConf().set(EngineConf.cfgkey_wfid, sdf.format(new Date()));
		String cfgProperties = staticProperties;
		if (this.getResourceSubFolder()!=null){
			cfgProperties = this.getResourceSubFolder() + staticProperties;
		}
		getConf().set(EngineConf.cfgkey_staticconfigfile, cfgProperties);
		getConf().set("mapreduce.output.textoutputformat.separator", ",");
		getConf().set("mapreduce.job.reduces", String.valueOf(numReducer));
		Job job = Job.getInstance(getConf(), "testCmd");
		job.setMapperClass(InvokeMapper.class);
		job.setReducerClass(etl.engine.InvokeReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(outputFormatClass);
		job.setInputFormatClass(inputFormatClass);
		FileInputFormat.setInputDirRecursive(job, true);
		
		for (Tuple2<String, String[]> rfifs: remoteFolderInputFiles){
			FileInputFormat.addInputPath(job, new Path(rfifs._1));
		}
		
		FileOutputFormat.setOutputPath(job, new Path(remoteOutputFolder));
		job.waitForCompletion(true);
		List<String> output;
		if (ParquetOutputFormat.class.isAssignableFrom(outputFormatClass)) {
			List<String> paths = HdfsUtil.listDfsFilePath(getFs(), remoteOutputFolder, true);
			
			output = new ArrayList<String>();
			for (String p: paths) {
				if (p.endsWith(".parquet")) {
					output.addAll(readParquetFile(p));
				}
			}
		} else
			output = HdfsUtil.stringsFromDfsFolder(getFs(), remoteOutputFolder);
		return output;
		
	}

	public List<String> readParquetFile(String p) throws Exception {
		ArrayList<String> output = new ArrayList<String>();
		MessageType schema;
		ParquetMetadata m;
		ParquetFileReader r;
		PageReadStore rowGroup;
		MessageColumnIO columnIO;
		RecordReader<Group> recordReader;
		List<Type> fields;
		int fieldIndex;
		StringBuilder buffer;
		PrimitiveType c;
		Binary b;
		m = ParquetFileReader.readFooter(getConf(), new Path(p), ParquetMetadataConverter.NO_FILTER);
		schema = m.getFileMetaData().getSchema();
		r = null;
		try {
			r = new ParquetFileReader(getConf(), m.getFileMetaData(), new Path(p), m.getBlocks(), schema.getColumns());
			rowGroup = r.readNextRowGroup();
			if (rowGroup != null) {
				columnIO = new ColumnIOFactory().getColumnIO(m.getFileMetaData().getSchema());
				recordReader = columnIO.getRecordReader(rowGroup, new GroupRecordConverter(schema));
				fields = schema.getFields();
				for (int i = 0; i < rowGroup.getRowCount(); i++) {
					final Group g = recordReader.read();
					fieldIndex = 0;
					buffer = new StringBuilder();
					for (Type f: fields) {
						c = f.asPrimitiveType();
						if (PrimitiveTypeName.BINARY.equals(c.getPrimitiveTypeName())) {
							buffer.append(g.getString(fieldIndex, 0));
						} else if (PrimitiveTypeName.INT32.equals(c.getPrimitiveTypeName())) {
							buffer.append(g.getInteger(fieldIndex, 0));
						} else if (PrimitiveTypeName.INT64.equals(c.getPrimitiveTypeName())) {
							if (OriginalType.TIMESTAMP_MILLIS.equals(c.getOriginalType())) {
								buffer.append(FieldType.sdatetimeRoughFormat.format(new Date(g.getLong(fieldIndex, 0))));
							} else if (OriginalType.DATE.equals(c.getOriginalType())) {
								buffer.append(FieldType.sdateFormat.format(new Date(g.getLong(fieldIndex, 0))));
							} else {
								buffer.append(g.getLong(fieldIndex, 0));
							}
						} else if (PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY.equals(c.getPrimitiveTypeName())) {
							b = g.getBinary(fieldIndex, 0);
							if (OriginalType.DECIMAL.equals(c.getOriginalType())) {
								buffer.append(new HiveDecimalWritable(b.getBytes(), c.getDecimalMetadata().getScale()).getHiveDecimal().toString());
							} else {
								buffer.append(b.toString());
							}
						} else if (PrimitiveTypeName.FLOAT.equals(c.getPrimitiveTypeName())) {
							buffer.append(g.getFloat(fieldIndex, 0));
						} else if (PrimitiveTypeName.DOUBLE.equals(c.getPrimitiveTypeName())) {
							buffer.append(g.getDouble(fieldIndex, 0));
						}
						fieldIndex ++;
						if (fieldIndex < fields.size())
							buffer.append(",");
					}
					output.add(buffer.toString());
				}
			}
		} finally {
			if (r != null)
				r.close();
		}
		return output;
	}

	public List<String> mrTest(List<Tuple2<String, String[]>> remoteFolderInputFiles, String remoteOutputFolder,
			String staticProperties, String cmdClassName, Class<? extends InputFormat> inputFormatClass,
			Class<? extends OutputFormat> outputFormatClass) throws Exception {
		return mrTest(remoteFolderInputFiles, remoteOutputFolder, staticProperties, cmdClassName, inputFormatClass, outputFormatClass, 1);
	}
	
	public List<String> mrTest(List<Tuple2<String, String[]>> remoteFolderInputFiles, String remoteOutputFolder,
			String staticProperties, String cmdClassName, Class<? extends InputFormat> inputFormatClass, int numReducer) throws Exception {
		return mrTest(remoteFolderInputFiles, remoteOutputFolder, staticProperties, cmdClassName, inputFormatClass, TextOutputFormat.class, numReducer);
		
	}
	
	public List<String> mrTest(List<Tuple2<String, String[]>> remoteFolderInputFiles, String remoteOutputFolder,
			String staticProperties, String cmdClassName, Class<? extends InputFormat> inputFormatClass) throws Exception {
		return mrTest(remoteFolderInputFiles, remoteOutputFolder, staticProperties, cmdClassName, inputFormatClass, TextOutputFormat.class, 1);
	}
	
	public List<String> mrTest(List<Tuple2<String, String[]>> remoteFolderInputFiles, String remoteOutputFolder,
			String staticProperties, String cmdClassName, boolean useFileNames) throws Exception {
		if (useFileNames)
			return mrTest(remoteFolderInputFiles, remoteOutputFolder, staticProperties, cmdClassName, FilenameInputFormat.class);
		else
			return mrTest(remoteFolderInputFiles, remoteOutputFolder, staticProperties, cmdClassName, NLineInputFormat.class);
	}
	
	public List<String> mrTest(String remoteInputFolder, String remoteOutputFolder, String staticProperties, 
			String[] inputDataFiles, String cmdClassName, boolean useFileNames) throws Exception {
		List<Tuple2<String, String[]>> rfifs = new ArrayList<Tuple2<String, String[]>>();
		rfifs.add(new Tuple2<String, String[]>(remoteInputFolder, inputDataFiles));
		if (useFileNames)
			return mrTest(rfifs, remoteOutputFolder, staticProperties, cmdClassName, FilenameInputFormat.class);
		else
			return mrTest(rfifs, remoteOutputFolder, staticProperties, cmdClassName, NLineInputFormat.class);
	}
	
	//spark test for cmd
	public List<String> sparkTestKV(String remoteInputFolder, String[] inputDataFiles, 
		String cmdProperties, Class<? extends ETLCmd> cmdClass, InputFormatType ift) throws Exception{
		return sparkTestKV(remoteInputFolder, inputDataFiles, cmdProperties, cmdClass, ift, null, false);
	}
	public List<String> sparkTestKVKeys(String remoteInputFolder, String[] inputDataFiles, 
		String cmdProperties, Class<? extends ETLCmd> cmdClass, InputFormatType ift) throws Exception{
		return sparkTestKV(remoteInputFolder, inputDataFiles, cmdProperties, cmdClass, ift, null, true);
	}
	
	public List<String> sparkTestKV(String remoteInputFolder, String[] inputDataFiles, 
			String cmdProperties, Class<? extends ETLCmd> cmdClass, InputFormatType ift, 
			Map<String, String> addConf, boolean key) throws Exception{
		SparkSession spark = SparkSession.builder().appName("wfName").master("local[5]").getOrCreate();
		SparkContext sc = spark.sparkContext();
		JavaSparkContext jsc = new JavaSparkContext(sc);
		try {
			getFs().delete(new Path(remoteInputFolder), true);
			List<String> inputPaths = new ArrayList<String>();
			for (String inputFile : inputDataFiles) {
				getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + inputFile), new Path(remoteInputFolder + inputFile));
				inputPaths.add(remoteInputFolder + inputFile);
			}
			if (InputFormatType.FileName == ift){
				inputPaths.clear();
				inputPaths.add(remoteInputFolder);
			}
			Constructor<? extends ETLCmd> constr = cmdClass.getConstructor(String.class, String.class, String.class, String.class, String[].class);
			String cfgProperties = cmdProperties;
			if (this.getResourceSubFolder()!=null){
				cfgProperties = this.getResourceSubFolder() + cmdProperties;
			}
			String wfName = "wfName";
			ETLCmd cmd = constr.newInstance(wfName, "wfId", cfgProperties, this.getDefaultFS(), null);
			if (addConf!=null){
				cmd.copyConf(addConf);
			}
			JavaPairRDD<String, String> result = cmd.sparkProcessFilesToKV(jsc.parallelize(inputPaths), jsc, ift, spark);
			//List<String> keys = result.keys().collect();
			
			List<String> ret = new ArrayList<String>();
			if (key){
				List<String> keys = result.keys().collect();
				ret.addAll(keys);
			}else{
				List<String> values = result.values().collect();
				ret.addAll(values);
			}
			return ret;
		}finally{
			jsc.close();
		}
	}
	
/*************
 * ACmd Test utils
 */
	public List<String> sparkTestACmd(String remoteInputFolder, String[] inputDataFiles, 
		String cmdProperties, Class<? extends ACmd> cmdClass, InputFormatType ift) throws Exception{
		return sparkTestACmd(remoteInputFolder, inputDataFiles, cmdProperties, cmdClass, ift, false);
	}
	public List<String> sparkTestACmdKeys(String remoteInputFolder, String[] inputDataFiles, 
		String cmdProperties, Class<? extends ACmd> cmdClass, InputFormatType ift) throws Exception{
		return sparkTestACmd(remoteInputFolder, inputDataFiles, cmdProperties, cmdClass, ift, true);
	}
	
	public List<String> sparkTestACmd(String remoteInputFolder, String[] inputDataFiles, 
			String cmdProperties, Class<? extends ACmd> cmdClass, InputFormatType ift, boolean key) throws Exception{
		SparkSession spark = SparkSession.builder().appName("wfName").master("local[5]").getOrCreate();
		SparkContext sc = spark.sparkContext();
		JavaSparkContext jsc = new JavaSparkContext(sc);
		try {
			getFs().delete(new Path(remoteInputFolder), true);
			List<String> inputPaths = new ArrayList<String>();
			for (String inputFile : inputDataFiles) {
				getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + inputFile), new Path(remoteInputFolder + inputFile));
				inputPaths.add(remoteInputFolder + inputFile);
			}
			if (InputFormatType.FileName == ift){
				inputPaths.clear();
				inputPaths.add(remoteInputFolder);
			}
			Constructor<? extends ACmd> constr = cmdClass.getConstructor(String.class, String.class, String.class, String.class, String.class);
			String cfgProperties = cmdProperties;
			if (this.getResourceSubFolder()!=null){
				cfgProperties = this.getResourceSubFolder() + cmdProperties;
			}
			String wfName = "wfName";
			
			ACmd cmd = constr.newInstance(wfName, "wfId", cfgProperties, null, this.getDefaultFS());
			RDD<Tuple2<String, String>> result = cmd.sparkProcessFilesToKV(jsc.parallelize(inputPaths).rdd(), ift, spark);
			
			JavaRDD<Tuple2<String,String>> jresult = result.toJavaRDD();
			if (key){
				List<String> keys = jresult.map(new Function<Tuple2<String,String>, String>(){
					@Override
					public String call(Tuple2<String, String> v1) throws Exception {
						return v1._1;
					}
				}).collect();
				return keys;
			}else{
				List<String> values = jresult.map(new Function<Tuple2<String,String>, String>(){
					@Override
					public String call(Tuple2<String, String> v1) throws Exception {
						return v1._2;
					}
				}).collect();
				return values;
			}
		}finally{
			jsc.close();
		}
	}
	
	public JavaPairRDD<String, String> sparkTestKVRDD(JavaSparkContext jsc, String remoteInputFolder, String[] inputDataFiles, 
			String cmdProperties, Class<? extends ETLCmd> cmdClass, InputFormatType ift, 
			Map<String, String> addConf) throws Exception{
		getFs().delete(new Path(remoteInputFolder), true);
		List<String> inputPaths = new ArrayList<String>();
		for (String inputFile : inputDataFiles) {
			getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + inputFile), new Path(remoteInputFolder + inputFile));
			inputPaths.add(remoteInputFolder + inputFile);
		}
		if (InputFormatType.FileName == ift){
			inputPaths.clear();
			inputPaths.add(remoteInputFolder);
		}
		Constructor<? extends ETLCmd> constr = cmdClass.getConstructor(String.class, String.class, String.class, String.class, String[].class);
		String cfgProperties = cmdProperties;
		if (this.getResourceSubFolder()!=null){
			cfgProperties = this.getResourceSubFolder() + cmdProperties;
		}
		String wfName = "wfName";
		ETLCmd cmd = constr.newInstance(wfName, "wfId", cfgProperties, this.getDefaultFS(), null);
		if (addConf!=null){
			cmd.copyConf(addConf);
		}
		return cmd.sparkProcessFilesToKV(jsc.parallelize(inputPaths), jsc, ift, null);
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
	public abstract String getResourceSubFolder();
	
	public String getLocalFolder() {
		if (getResourceSubFolder()==null){
			return localFolder;
		}else
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

	public String getOozieUser() {
		return oozieUser;
	}

	public void setOozieUser(String oozieUser) {
		this.oozieUser = oozieUser;
	}

	public PropertiesConfiguration getPc() {
		return pc;
	}

	public void setPc(PropertiesConfiguration pc) {
		this.pc = pc;
	}
}
