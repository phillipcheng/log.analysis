package log.analysis.driver;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.cld.util.StringUtil;

public class MRProcessor extends Configured implements Tool {

	public static final Logger logger = Logger.getLogger(MRProcessor.class);
	public static final String PARAM_KEY="key";
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, LongWritable>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			logger.info(String.format("mapper input, key:%s, value:%s", key, value));
			context.write(value, (LongWritable)key);
			logger.info(String.format("mapper output, key:%s, value:%s", value, key));
		}
	}
	
	public static class MyPartitioner extends Partitioner<Text, LongWritable>{
		@Override
		public int getPartition(Text key, LongWritable value, int numPartitions) {
			logger.info(String.format("partitioner input, numPartitions:%d, key:%s, value:%s", numPartitions, key, value));
			int lineLength = key.getLength()+1;
			long lineNo = value.get() / lineLength;
			int partitionId = (int) (lineNo % numPartitions);
			logger.info(String.format("partitioner output, id:%d, key:%s, value:%s, lineNo: %d", partitionId, key, value, lineNo));
			return partitionId;
		}
	}
	
	public static class InvokeCombiner extends Reducer<Text,LongWritable,Void,Void> {
		public void reduce(Text key, Iterable<LongWritable> values, Context context ) throws IOException, InterruptedException {
			logger.info(String.format("in combiner, key:%s, values:%s", key, values));
		}
	}
	
	public static class InvokeReducer extends Reducer<Text,LongWritable,Void,Void> {
		public void reduce(Text key, Iterable<LongWritable> values, Context context ) throws IOException, InterruptedException {
			logger.info(String.format("in reducer, key:%s, values:%s", key, values));
			//start the script configured passing the parameters
			try{
				MRPreloadConfig mrpc = new MRPreloadConfig("mrpreload.properties");
				String command = mrpc.getMrCommand();
				Map<String, Object> params = new HashMap<String, Object>();
				params.put(PARAM_KEY, key.toString());
				command = StringUtil.fillParams(command, params, "$", "");
				logger.info(String.format("mr command is %s", command));
				CommandLine cmdLine = CommandLine.parse(command);
				DefaultExecutor executor = new DefaultExecutor();
				int exitValue = executor.execute(cmdLine);
				logger.info(String.format("process for key:%s ended with exitValue %d.", key, exitValue));
			}catch(Exception e){
				logger.error("", e);
			}
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		System.out.println("args in main:" + Arrays.toString(args));
		// Let ToolRunner handle generic command-line options 
        int res = ToolRunner.run(new Configuration(), new MRProcessor(), args);
        System.exit(res);
	}


	public int run(String[] args) throws Exception {
		System.out.println("args in run:" + Arrays.toString(args));
		Configuration conf = getConf();
		
		Job job = Job.getInstance(conf, "MRProcessor");
		job.setJarByClass(MRProcessor.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setPartitionerClass(MyPartitioner.class);
		job.setReducerClass(InvokeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		Path in = new Path(args[0]);
		FileInputFormat.addInputPath(job, in);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}
}
