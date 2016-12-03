package etl.util;

import java.io.IOException;
import java.nio.charset.Charset;

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

public class XmlRecordReader extends RecordReader<LongWritable, Text> {
	public static final String START_TAG_KEY = "xmlinput.start";
	public static final String END_TAG_KEY = "xmlinput.end";
	
	private static final byte[] EMPTY_BYTES = new byte[0];
	private byte[] startTag;
	private byte[] endTag;
	private long start;
	private long end;
	private FSDataInputStream fsin;
	private DataOutputBuffer buffer;
	private String header;
	private String footer;

	private LongWritable key = new LongWritable();
	private Text value = new Text();

	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		if (conf.get(START_TAG_KEY) != null)
			startTag = conf.get(START_TAG_KEY).getBytes("utf-8");
		else
			startTag = EMPTY_BYTES;
		if (conf.get(END_TAG_KEY) != null)
			endTag = conf.get(END_TAG_KEY).getBytes("utf-8");
		else
			endTag = EMPTY_BYTES;
		FileSplit fileSplit = (FileSplit) split;

		// open the file and seek to the start of the split
		start = fileSplit.getStart();
		end = start + fileSplit.getLength();
		Path file = fileSplit.getPath();
		FileSystem fs = file.getFileSystem(conf);
		fsin = fs.open(fileSplit.getPath());
		fsin.seek(start);
		
		buffer = new DataOutputBuffer();
		
		if (readUntilMatch(startTag, true))
			header = new String(buffer.getData(), 0, buffer.getLength(), Charset.forName("utf8"));
		else
			header = "";
		
		buffer.reset();
		
		while (fsin.getPos() < end) {
			/* Seek to the last record to get the footer */
			buffer.reset();
			readUntilMatch(endTag, true);
		}
		
		footer = new String(buffer.getData(), 0, buffer.getLength(), Charset.forName("utf8"));
		
		buffer.reset();
		
		/* Seek to the beginning again */
		fsin.seek(start);
	}

	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (fsin.getPos() < end) {
			if (readUntilMatch(startTag, false)) {
				try {
					if (readUntilMatch(endTag, true)) {
						key.set(fsin.getPos());
						value.set(header);
						value.append(buffer.getData(), 0, buffer.getLength());
						byte[] footerBuf = footer.getBytes(Charset.forName("utf8"));
						value.append(footerBuf, 0, footerBuf.length);
						return true;
					}
				} finally {
					buffer.reset();
				}
			}
		}
		return false;
	}

	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	public void close() throws IOException {
		if (buffer != null) {
			buffer.close();
			buffer = null;
		}
		if (fsin != null) {
			fsin.close();
			fsin = null;
		}
	}

	public float getProgress() throws IOException {
		return (fsin.getPos() - start) / (float) (end - start);
	}

	private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
		int i = 0;
		
		if (match.length == 0)
			return true;
		
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