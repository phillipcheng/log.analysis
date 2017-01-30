package etl.input;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class LTSequenceFileInputFormat extends SequenceFileInputFormat<LongWritable, Text>{

}
