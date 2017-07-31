package etl.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StandardXmlRecordReader extends RecordReader<LongWritable, Text> {

	public static final Logger logger = LogManager.getLogger(StandardXmlRecordReader.class);

	public static final String START_TAG_KEY = "xmlinput.start";
	public static final String END_TAG_KEY = "xmlinput.end";
	public static final String START_ROW_TAG_KEY = "xmlinput.row.start";
	public static final String END_ROW_TAG_KEY = "xmlinput.row.end";

	private byte[] startTag;
	private byte[] endTag;
	private long start;
	private long end;
	private FSDataInputStream fsin;
	private DataOutputBuffer buffer = new DataOutputBuffer();

	private LongWritable key = new LongWritable();
	private Text value = new Text();

	@Override
	public void initialize(InputSplit is, TaskAttemptContext context) throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();

		if (conf.get(START_TAG_KEY) != null)
			startTag = conf.get(START_TAG_KEY).getBytes("utf-8");
		if (conf.get(END_TAG_KEY) != null)
			endTag = conf.get(END_TAG_KEY).getBytes("utf-8");
		FileSplit fileSplit = (FileSplit) is;
		start = fileSplit.getStart();
		end = start + fileSplit.getLength();
		Path file = fileSplit.getPath();
		FileSystem fs = file.getFileSystem(conf);
		fsin = fs.open(fileSplit.getPath());
		fsin.seek(start);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (fsin.getPos() < end) {
			if (readUntilMatch(startTag, false)) {
				try {
					buffer.write(startTag);
					if (readUntilMatch(endTag, true)) {
						value.set(buffer.getData(), 0, buffer.getLength());
						key.set(fsin.getPos());
						return true;
					}
				} finally {
					buffer.reset();
				}
			}
		}
		return false;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (fsin.getPos() - start) / (float) (end - start);
	}

	@Override
	public void close() throws IOException {
		fsin.close();
	}

	private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
		int i = 0;
		while (true) {
			int b = fsin.read();
			// end of file:
			if (b == -1)
				return false;
			// save to buffer:
			if (withinBlock)
				buffer.write(b);

			// check if we're matching:
			if (b == match[i]) {
				i++;
				if (i >= match.length)
					return true;
			} else
				i = 0;
			// see if we've passed the stop point:
			if (!withinBlock && i == 0 && fsin.getPos() >= end)
				return false;
		}
	}

}