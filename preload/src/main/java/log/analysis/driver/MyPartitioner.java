package log.analysis.driver;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Logger;

public class MyPartitioner extends Partitioner<Text, LongWritable>{
	public static final Logger logger = Logger.getLogger(MyPartitioner.class);
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