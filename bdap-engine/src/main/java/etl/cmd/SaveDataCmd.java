package etl.cmd;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.script.CompiledScript;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.task.ReduceContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.Progress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkHadoopWriter;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import etl.engine.ETLCmd;
import etl.engine.types.InputFormatType;
import etl.engine.types.ProcessMode;
import etl.output.ParquetOutputFormat;
import etl.output.RDDMultipleTextOutputFormat;
import etl.util.ConfigKey;
import etl.util.ScriptEngineUtil;
import scala.Function2;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction2;

public class SaveDataCmd extends SchemaETLCmd {
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LogManager.getLogger(SaveDataCmd.class);
	
	public static final @ConfigKey String cfgkey_log_tmp_dir="log.tmp.dir";
	public static final @ConfigKey String cfgkey_table_names="table.names";
	
	private String logTmpDir;
	private etl.engine.types.OutputFormat outputFormat = etl.engine.types.OutputFormat.text;
	private String[] tableNames;
	
	public SaveDataCmd(){
		super();
	}
	
	public SaveDataCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	public SaveDataCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs, ProcessMode pm){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, pm);
	}
	
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs, ProcessMode pm){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
		String tmpDirExp = this.getPc().getString(cfgkey_log_tmp_dir);
		logger.info(String.format("%s_exp:%s", cfgkey_log_tmp_dir, tmpDirExp));
		if (tmpDirExp!=null){
			CompiledScript cs = ScriptEngineUtil.compileScript(tmpDirExp);
			logTmpDir = ScriptEngineUtil.eval(cs, super.getSystemVariables());
			logger.info(String.format("%s:%s", cfgkey_log_tmp_dir, logTmpDir));
		}
		outputFormat = etl.engine.types.OutputFormat.valueOf(super.getCfgString(cfgkey_output_file_format, etl.engine.types.OutputFormat.text.toString()));
		tableNames = super.getPc().getStringArray(cfgkey_table_names);
	}
	
	@Override
	public boolean hasReduce(){
		return false;
	}
	@Override
	public boolean useSparkSql(){
		return false;
	}
	
	//do not really save file, generate the list of (key->file names)
	public Map<String, Object> mapProcess(long offset, String row, 
			Mapper<LongWritable, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos) throws Exception {
		if (super.inputFormatType==InputFormatType.CombineFileName || inputFormatType==InputFormatType.FileName){
			//each row is the file name
			context.write(new Text(row), null);
		}else{
			if (offset==0){
				String key = getTableNameSetFileNameByContext(context);
				String value = (String) getSystemVariables().get(VAR_NAME_PATH_NAME);
				context.write(new Text(key), new Text(value));
			}
		}
		return null;
	}
	
	private String saveRDD(SparkSession spark, JavaRDD<Row> srcRDD, String key){
		int num = 0;
		if (ETLCmd.SINGLE_TABLE.equals(key)){
			num = srcRDD.take(1).get(0).size();
		}
		StructType st = getSparkSqlSchema(key, num);
		if (st!=null){
			String fileName = String.format("%s%s%s", defaultFs, logTmpDir, key);
			Dataset<Row> dfr = spark.createDataFrame(srcRDD, st);
			if (outputFormat==etl.engine.types.OutputFormat.text){
				dfr.write().csv(fileName);
			}else if (outputFormat==etl.engine.types.OutputFormat.parquet){
				dfr.write().parquet(fileName);
			}else{
				logger.error(String.format("output format %s not supported.", outputFormat));
			}
			return fileName;
		}else{
			logger.error(String.format("schema not found for %s", key));
			return null;
		}
	}
	
	private String saveList(SparkSession spark, List<Row> srcRDD, String key){
		int num = srcRDD.get(0).size();
		StructType st = getSparkSqlSchema(key, num);
		if (st!=null){
			String fileName = String.format("%s%s%s", defaultFs, logTmpDir, key);
			Dataset<Row> dfr = spark.createDataFrame(srcRDD, st);
			if (outputFormat==etl.engine.types.OutputFormat.text){
				dfr.write().csv(fileName);
			}else if (outputFormat==etl.engine.types.OutputFormat.parquet){
				dfr.write().parquet(fileName);
			}else{
				logger.error(String.format("output format %s not supported.", outputFormat));
			}
			return fileName;
		}else{
			logger.error(String.format("schema not found for %s", key));
			return null;
		}
	}
	
	@Override
	public JavaPairRDD<String,String> dataSetProcess(JavaSparkContext jsc, SparkSession spark, JavaPairRDD<String, Row> rowRDD){
		//TODO using filter, only test function, not scalable
		//rowRDD.cache();
		JavaRDD<String> distinctKeys = rowRDD.keys().distinct();
		List<String> keys = null;
		if (tableNames.length>0){
			keys = Arrays.asList(tableNames);
		}else{
			keys = distinctKeys.collect();
		}
		JavaPairRDD<String, Row> srcRDD = rowRDD;
		int i=0;
		while (i<keys.size()){
			String key = keys.get(i);
			i++;
			JavaRDD<Row> frdd = filterPairRDDRow(srcRDD, key);
			saveRDD(spark, frdd, key);
			//srcRDD = filterPairRDDRowNot(srcRDD, key);
		}
		return getFiles(distinctKeys, false);
	}
	
	private java.util.Iterator<Tuple2<String, String>> getFilesByKey(String key, boolean file) throws Exception{
		String path = null;
		if (file){
			path = String.format("%s%s%s-*", getDefaultFs(), logTmpDir, key);
		}else{
			path = String.format("%s%s%s/part-*", getDefaultFs(), logTmpDir, key);
		}
		FileStatus[] fstatlist = fs.globStatus(new Path(path));
		List<Tuple2<String,String>> ret = new ArrayList<Tuple2<String,String>>();
		for (FileStatus fstat:fstatlist){
			logger.debug(String.format("save cmd, key:%s, path:%s", key, fstat.getPath().toString()));
			ret.add(new Tuple2<String, String>(key, fstat.getPath().toString()));
		}
		return ret.iterator();
	}
	
	private JavaPairRDD<String,String> getFiles(JavaRDD<String> keys, boolean file){
		return keys.flatMapToPair(new PairFlatMapFunction<String, String, String>(){
			private static final long serialVersionUID = 1L;
			@Override
			public java.util.Iterator<Tuple2<String, String>> call(String t) throws Exception {
				init();
				return getFilesByKey(t, file);
			}
		});
	}
	
	@Override
	public JavaPairRDD<String, String> sparkProcessKeyValue(JavaPairRDD<String, String> input, JavaSparkContext jsc, 
			InputFormatType ift, SparkSession spark){
		logger.info(String.format("%s:%s", cfgkey_log_tmp_dir, logTmpDir));
		if (useSparkSql() && outputFormat==etl.engine.types.OutputFormat.parquet){
			return super.sparkProcessKeyValue(input, jsc, ift, spark);
		}else{
			copyConf();
			if (outputFormat==etl.engine.types.OutputFormat.parquet){
				saveAsNewAPIHadoopFile(String.format("%s%s", super.getDefaultFs(), logTmpDir), getHadoopConf(), input, ParquetOutputFormat.class);
			}else{
				logger.info(String.format("save as hadoop file: defaultFs:%s, dir:%s", super.getDefaultFs(), logTmpDir));
				input.saveAsHadoopFile(String.format("%s%s", super.getDefaultFs(), logTmpDir), Text.class, Text.class, 
						RDDMultipleTextOutputFormat.class);
			}
			return getFiles(input.keys().distinct(), true);
		}
	}
	
	public String getLogTmpDir() {
		return logTmpDir;
	}

	public void setLogTmpDir(String logTmpDir) {
		this.logTmpDir = logTmpDir;
	}

	
	private class SimpleIterator implements RawKeyValueIterator {
		private Iterator<Tuple2<String, String>> iter;
		public SimpleIterator(Iterator<Tuple2<String, String>> iter) {
			this.iter = iter;
		}

		public DataInputBuffer getKey() throws IOException {
			return null;
		}

		public DataInputBuffer getValue() throws IOException {
			return null;
		}

		public boolean next() throws IOException {
			return iter.hasNext();
		}

		public void close() throws IOException {
		}

		public Progress getProgress() {
			return null;
		}
	}
	
	private class SparkMultipleOutputFunction extends AbstractFunction2<TaskContext, Iterator<Tuple2<String, String>>, Void> implements Serializable {
		private static final long serialVersionUID = 1L;
		private transient Configuration conf;
		private ETLCmd parent;
		private JavaPairRDD<String, String> rdd;
		private Class<? extends OutputFormat<Void, Text>> outputFormatClass;
		private String jtid;
		private String outputPath;
		public SparkMultipleOutputFunction(ETLCmd parent, JavaPairRDD<String, String> rdd, Class<? extends OutputFormat<Void, Text>> outputFormatClass, String jtid, String outputPath) {
			this.conf = parent.getHadoopConf();
			this.parent = parent;
			this.rdd = rdd;
			this.outputFormatClass = outputFormatClass;
			this.jtid = jtid;
			this.outputPath = outputPath;
		}
		public Configuration getConf() {
			if (conf == null) {
				parent.init();
				parent.copyConf();
				parent.getHadoopConf().set(FileOutputFormat.OUTDIR, outputPath);
				parent.getHadoopConf().set(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, outputFormatClass.getCanonicalName());
				return parent.getHadoopConf();
			} else
				return conf;
		}
		public Void innerApply(TaskContext context, Iterator<Tuple2<String, String>> iter) throws Exception {
			TaskAttemptID attemptId = new TaskAttemptID(jtid, rdd.id(), TaskType.REDUCE, context.partitionId(), context.attemptNumber());
			TaskAttemptContextImpl hadoopContext = new TaskAttemptContextImpl(getConf(), attemptId);
			
			OutputFormat<Void, Text> outputFormat = outputFormatClass.newInstance();
			OutputCommitter committer = outputFormat.getOutputCommitter(hadoopContext);
					
			committer.setupTask(hadoopContext);
			
			RecordWriter<Void, Text> recordWriter = outputFormat.getRecordWriter(hadoopContext);
					 
			ReduceContextImpl<Text, Text, Void, Text> taskInputOutputContext = new ReduceContextImpl<Text, Text, Void, Text>(getConf(), attemptId, new SimpleIterator(iter), new GenericCounter(), new GenericCounter(),
					recordWriter, committer, new ReduceContextImpl.DummyReporter(), null, Text.class, Text.class);
			MultipleOutputs<Void, Text> writer = new MultipleOutputs<Void, Text>(taskInputOutputContext);
			
			try {
		        while (iter.hasNext()) {
		          Tuple2<String, String> pair = iter.next();
		          writer.write((Void)null, new Text(pair._2), pair._1);
		        }
		    } finally {
		        writer.close();
				
				recordWriter.close(taskInputOutputContext);
		    }
			
			committer.commitTask(hadoopContext);
			
			return null;
		}
		public Void apply(TaskContext context, Iterator<Tuple2<String, String>> iter) {
			try {
				return this.innerApply(context, iter);
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
				return null;
			}
		}
	}
	
	private void saveAsNewAPIHadoopFile(String outputPath, Configuration conf, JavaPairRDD<String, String> rdd, Class<? extends OutputFormat<Void, Text>> outputFormatClass) {
		try {
			OutputFormat<Void, Text> outputFormat = outputFormatClass.newInstance();
			Date currentDate = new Date();
			JobID jid = SparkHadoopWriter.createJobID(currentDate, rdd.id());
			Job job = Job.getInstance(conf);
			job.getConfiguration().set(FileOutputFormat.OUTDIR, outputPath);
			job.getConfiguration().set(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, outputFormatClass.getCanonicalName());
			
			//jobTaskContext = new TaskAttemptContextImpl(conf, jobAttemptId)
			//outputFormat.getOutputCommitter(context)
			Function2<TaskContext, Iterator<Tuple2<String, String>>, Void> func = new SparkMultipleOutputFunction(this, rdd, outputFormatClass, jid.getJtIdentifier(), outputPath);
			TaskAttemptID jobAttemptId = new TaskAttemptID(jid.getJtIdentifier(), rdd.id(), TaskType.MAP, 0, 0);
			TaskAttemptContextImpl jobTaskContext = new TaskAttemptContextImpl(job.getConfiguration(), jobAttemptId);
			OutputCommitter jobCommitter = outputFormat.getOutputCommitter(jobTaskContext);
			jobCommitter.setupJob(jobTaskContext);
			rdd.context().runJob(rdd.rdd(), func, scala.reflect.ClassManifestFactory.fromClass(Void.class));
			jobCommitter.commitJob(jobTaskContext);
			
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}
}
