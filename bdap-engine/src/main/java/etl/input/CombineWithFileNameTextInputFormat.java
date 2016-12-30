package etl.input;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReaderWrapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class CombineWithFileNameTextInputFormat extends CombineFileInputFormat<LongWritable,Text> {

	public static final String filename_value_sep=",";
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException {
		return new CombineFileRecordReader<LongWritable,Text>(
			      (CombineFileSplit)split, context, TextRecordReaderWrapper.class);
	}
	
	private static class TextRecordReaderWrapper extends CombineFileRecordReaderWrapper<LongWritable,Text> {
		private String curFilePath;
		// this constructor signature is required by CombineFileRecordReader
		public TextRecordReaderWrapper(CombineFileSplit split, TaskAttemptContext context, Integer idx) throws IOException, InterruptedException {
			super(new TextInputFormat(), split, context, idx);
		}
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			super.initialize(split, context);
			curFilePath = context.getConfiguration().get(MRJobConfig.MAP_INPUT_FILE);
		}
		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			Text value = super.getCurrentValue();
			return new Text(String.format("%s%s%s", curFilePath, filename_value_sep, value.toString()));
		}
	}
}
