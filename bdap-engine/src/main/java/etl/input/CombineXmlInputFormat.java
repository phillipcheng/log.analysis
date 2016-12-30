package etl.input;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReaderWrapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

public class CombineXmlInputFormat extends CombineFileInputFormat<LongWritable,Text> {
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException {
		return new CombineFileRecordReader<LongWritable,Text>(
			      (CombineFileSplit)split, context, TextRecordReaderWrapper.class);
	}
	
	private static class TextRecordReaderWrapper extends CombineFileRecordReaderWrapper<LongWritable,Text> {
		// this constructor signature is required by CombineFileRecordReader
		public TextRecordReaderWrapper(CombineFileSplit split, TaskAttemptContext context, Integer idx) throws IOException, InterruptedException {
			super(new XmlInputFormat(), split, context, idx);
		}
	}
}
