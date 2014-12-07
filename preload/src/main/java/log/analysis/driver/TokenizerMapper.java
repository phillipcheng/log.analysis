package log.analysis.driver;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class TokenizerMapper extends Mapper<Object, Text, Text, LongWritable>{
	public static final Logger logger = Logger.getLogger(TokenizerMapper.class);
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		logger.info(String.format("mapper input, key:%s, value:%s", key, value));
		context.write(value, (LongWritable)key);
		logger.info(String.format("mapper output, key:%s, value:%s", value, key));
	}
}