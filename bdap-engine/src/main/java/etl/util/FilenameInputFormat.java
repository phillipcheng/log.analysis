package etl.util;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;



/**
 * Handy "input format" which maps the input filename into a "record"
 * which just has the filename.
 *
 * This is very useful for map-reduce jobs where you want to pass the
 * filenames into the map() function.  Use this as the input format,
 * and the input filenames will be passed to the map().  The full
 * pathname is given as both the key and the value to the map().
 */
public class FilenameInputFormat extends FileInputFormat<LongWritable,Text> {
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		return new RecordReader<LongWritable, Text>(){
			boolean done = false;
			Path fileName;
			@Override
			public void initialize(InputSplit split, TaskAttemptContext context)
					throws IOException, InterruptedException {
				FileSplit fsplit = (FileSplit)split;
				fileName = fsplit.getPath();
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
		};
	}
}