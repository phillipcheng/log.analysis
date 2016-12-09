package etl.mapred.test;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class OldMapper implements Mapper<LongWritable, Text, Text, Text>{

	public static final Logger logger = LogManager.getLogger(OldMapper.class);
	
	@Override
	public void configure(JobConf job) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		logger.info(value.toString());
		output.collect(value, value);
	}
}
