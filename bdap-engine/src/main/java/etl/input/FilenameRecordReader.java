package etl.input;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FilenameRecordReader extends RecordReader<LongWritable, Text> {
	boolean done = false;
	Path fileName;
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		fileName = ((FileSplit)split).getPath();
		done = false;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (done){
			return false;
		}else{
			done = true;
			return true;
		}
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return new LongWritable(0);
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return new Text(fileName.toString());
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public void close() throws IOException {
	}

}
