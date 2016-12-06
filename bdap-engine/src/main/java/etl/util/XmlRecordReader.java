package etl.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;
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
	public static final String START_ROW_TAG_KEY = "xmlinput.row.start";
	public static final String END_ROW_TAG_KEY = "xmlinput.row.end";
	public static final String MAX_ROW_NUMBER = "xmlinput.row.max.number";
	
	private static final byte[] EMPTY_BYTES = new byte[0];
	private byte[] startTag;
	private byte[] endTag;
	private byte[] rowStartTag;
	private byte[] rowEndTag;
	private long rowMaxNumber;
	private long start;
	private long end;
	private XmlInputStream fsin;
	private XmlInputStream currentSectionIn;
	private DataOutputBuffer buffer;
	private String header;
	private String footer;
	private String sectionHeader;
	private String sectionFooter;

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
		if (conf.get(START_ROW_TAG_KEY) != null)
			rowStartTag = conf.get(START_ROW_TAG_KEY).getBytes("utf-8");
		else
			rowStartTag = null; /* Disable row read */
		if (conf.get(END_ROW_TAG_KEY) != null)
			rowEndTag = conf.get(END_ROW_TAG_KEY).getBytes("utf-8");
		else
			rowEndTag = null; /* Disable row read */
		
		rowMaxNumber = conf.getLong(MAX_ROW_NUMBER, Long.MAX_VALUE);

		FileSplit fileSplit = (FileSplit) split;

		// open the file and seek to the start of the split
		start = fileSplit.getStart();
		end = start + fileSplit.getLength();
		Path file = fileSplit.getPath();
		FileSystem fs = file.getFileSystem(conf);
		fsin = new XmlInputStream(fs.open(fileSplit.getPath()), start, end);
		fsin.seek(start);
		
		buffer = new DataOutputBuffer();
		
		if (readUntilMatch(fsin, startTag, true))
			header = new String(buffer.getData(), 0, buffer.getLength(), Charset.forName("utf8"));
		else
			header = "";
		
		while (fsin.getPos() < end) {
			/* Seek to the last record to get the footer */
			buffer.reset();
			readUntilMatch(fsin, endTag, true);
		}
		
		footer = new String(buffer.getData(), 0, buffer.getLength(), Charset.forName("utf8"));
		
		buffer.reset();
		
		/* Seek to the beginning again */
		fsin.seek(start);
		
		if (this.rowStartTag != null && this.rowEndTag != null)
			currentSectionIn = nextSection(fsin, startTag, endTag, rowStartTag, rowEndTag);
	}
	
	private boolean nextKeyValue(XmlInputStream dataIn, byte[] startTag, byte[] endTag, String header, String footer, long count) throws IOException {
		int i = 0;
		
		byte[] headerBuf = header.getBytes(Charset.forName("utf8"));
		value.append(headerBuf, 0, headerBuf.length);
		
		do {
			if (readUntilMatch(dataIn, startTag, i > 0)) {
				try {
					if (readUntilMatch(dataIn, endTag, true)) {
						value.append(buffer.getData(), 0, buffer.getLength());
						i = i + 1;
					}
				} finally {
					buffer.reset();
				}
			}
		} while (dataIn.getPos() < dataIn.getEnd() && i < count);

		key.set(dataIn.getPos());
		
		byte[] footerBuf = footer.getBytes(Charset.forName("utf8"));
		value.append(footerBuf, 0, footerBuf.length);
		
		return i > 0;
	}

	private XmlInputStream nextSection(XmlInputStream dataIn, byte[] startTag, byte[] endTag, byte[] rowStartTag, byte[] rowEndTag) throws IOException {
		if (readUntilMatch(dataIn, startTag, false)) {
			try {
				byte[] bytes;
				long start = dataIn.getPos();
				if (readUntilMatch(dataIn, endTag, true)) {
					bytes = new byte[buffer.getLength()];
					System.arraycopy(buffer.getData(), 0, bytes, 0, buffer.getLength());
					XmlInputStream sectionIn = new XmlInputStream(new ByteArrayInputStream(bytes, 0, bytes.length), start, start + bytes.length);
					
					buffer.reset();
					
					if (readUntilMatch(sectionIn, rowStartTag, true))
						sectionHeader = new String(buffer.getData(), 0, buffer.getLength(), Charset.forName("utf8"));
					else
						sectionHeader = "";
					
					do {
						/* Seek to the last record to get the footer */
						buffer.reset();
					} while (readUntilMatch(sectionIn, rowEndTag, true));
					
					sectionFooter = new String(buffer.getData(), 0, buffer.getLength(), Charset.forName("utf8"));
					
					sectionIn.reset();
					
					return sectionIn;
				}
			} finally {
				buffer.reset();
			}
		}
		return null;
	}

	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (this.rowStartTag != null && this.rowEndTag != null) {
			boolean available;
			value.set(header);
			if (currentSectionIn != null)
				available = nextKeyValue(currentSectionIn, rowStartTag, rowEndTag, sectionHeader, sectionFooter, rowMaxNumber);
			else
				available = false;
			if (available) {
				byte[] footerBuf = footer.getBytes(Charset.forName("utf8"));
				value.append(footerBuf, 0, footerBuf.length);
				return true;
			} else if (currentSectionIn != null) {
				currentSectionIn = nextSection(fsin, startTag, endTag, rowStartTag, rowEndTag);
				if (currentSectionIn != null) {
					value.set(header);
					available = nextKeyValue(currentSectionIn, rowStartTag, rowEndTag, sectionHeader, sectionFooter, rowMaxNumber);
					if (available) {
						byte[] footerBuf = footer.getBytes(Charset.forName("utf8"));
						value.append(footerBuf, 0, footerBuf.length);
						return true;
					} else {
						return false;
					}
				} else {
					return false;
				}
			} else {
				return false;
			}
		} else {
			value.clear();
			return nextKeyValue(fsin, startTag, endTag, header, footer, 1);
		}
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

	private boolean readUntilMatch(XmlInputStream dataIn, byte[] match, boolean withinBlock) throws IOException {
		int i = 0;
		
		if (match.length == 0)
			return true;
		
		dataIn.setWithinBlock(withinBlock);
		
		while (true) {
			int b = dataIn.read();
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
			
			dataIn.setMatching(i != 0);
		}
	}
}