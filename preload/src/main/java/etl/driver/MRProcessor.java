package etl.driver;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class MRProcessor extends Configured implements Tool {

	public static final Logger logger = Logger.getLogger(MRProcessor.class);
	
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
		job.setMapperClass(InvokeMapper.class);
		job.setInputFormatClass(NLineInputFormat.class);
		job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 1);
		job.setOutputFormatClass(NullOutputFormat.class);
		Path in = new Path(args[0]);
		FileInputFormat.addInputPath(job, in);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}
}