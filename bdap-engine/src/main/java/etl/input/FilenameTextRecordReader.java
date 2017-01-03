package etl.input;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class FilenameTextRecordReader extends RecordReader<Text, Text> {
	
	String fileName;
	LineRecordReader lrr;
	
	public FilenameTextRecordReader(){
		lrr = new LineRecordReader();
		
	}
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		lrr.initialize(split, context);
		fileName =((FileSplit) split).getPath().toString();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return lrr.nextKeyValue();
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return new Text(fileName);
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return lrr.getCurrentValue();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return lrr.getProgress();
	}

	@Override
	public void close() throws IOException {
		lrr.close();
	}
}
