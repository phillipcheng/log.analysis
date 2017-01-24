package etl.cmd;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
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
import org.apache.hadoop.mapreduce.InputFormat;
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
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.spark.SparkHadoopWriter;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.SparkSession;

import etl.engine.ETLCmd;
import etl.engine.InputFormatType;
import etl.engine.ProcessMode;
import etl.output.ParquetOutputFormat;
import etl.spark.RDDMultipleTextOutputFormat;
import etl.util.ConfigKey;
import etl.util.ScriptEngineUtil;
import scala.Function2;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction2;

public class SaveDataCmd extends SchemaETLCmd {
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LogManager.getLogger(SaveDataCmd.class);
	//cfgkey
	public static final @ConfigKey String cfgkey_log_tmp_dir="log.tmp.dir";
	
	private String logTmpDir;
	
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
			RecordWriter<Void, Text> recordWriter = outputFormat.getRecordWriter(hadoopContext);
					
			committer.setupTask(hadoopContext);
					 
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
		    }
			
			recordWriter.close(taskInputOutputContext);
			
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
	}
	
	public boolean hasReduce(){
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
	
	public JavaPairRDD<String, String> sparkProcessKeyValue(JavaPairRDD<String, String> input, JavaSparkContext jsc, 
			Class<? extends InputFormat> inputFormatClass, SparkSession spark){
		logger.info(String.format("%s:%s", cfgkey_log_tmp_dir, logTmpDir));
		copyConf();
		if (ParquetInputFormat.class.isAssignableFrom(inputFormatClass)) {
			saveAsNewAPIHadoopFile(String.format("%s%s", super.getDefaultFs(), logTmpDir), getHadoopConf(), input, ParquetOutputFormat.class);
			
			
			/* input.mapToPair(new PairFunction<Tuple2<String, String>, Void, Text>() {
				private static final long serialVersionUID = 1L;
				public Tuple2<Void, Text> call(Tuple2<String, String> t) throws Exception {
					return new Tuple2<Void, Text>(null, new Text(t._2));
				}
			}).saveAsNewAPIHadoopFile(String.format("%s%s", super.getDefaultFs(), logTmpDir), Void.class, Text.class, ParquetOutputFormat.class, getHadoopConf()); */
		} else
			input.saveAsHadoopFile(String.format("%s%s", super.getDefaultFs(), logTmpDir), Text.class, Text.class, RDDMultipleTextOutputFormat.class);
		//TODO use input.saveAsNewAPIHadoopFile(path, keyClass, valueClass, outputFormatClass);
		return input.groupByKey().keys().flatMapToPair(new PairFlatMapFunction<String, String, String>(){
			private static final long serialVersionUID = 1L;
			@Override
			public java.util.Iterator<Tuple2<String, String>> call(String t) throws Exception {
				init();
				String path = String.format("%s%s%s-*", getDefaultFs(), logTmpDir, t);
				FileStatus[] fstatlist = fs.globStatus(new Path(path));
				List<Tuple2<String,String>> ret = new ArrayList<Tuple2<String,String>>();
				for (FileStatus fstat:fstatlist){
					logger.debug(String.format("save cmd, key:%s, path:%s", t, fstat.getPath().toString()));
					ret.add(new Tuple2<String, String>(t, fstat.getPath().toString()));
				}
				return ret.iterator();
			}
		});
	}
	
	public String getLogTmpDir() {
		return logTmpDir;
	}

	public void setLogTmpDir(String logTmpDir) {
		this.logTmpDir = logTmpDir;
	}
}
