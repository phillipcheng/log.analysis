package etl.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class KCVInputFormat extends TextInputFormat {
	public static final String HEADER_LINE_KEY = "header.line";
	public static final String FOOTER_LINE_KEY = "footer.line";

	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
		return new KCVRecordReader();
	}

	/**
	 * XMLRecordReader class to read through a given xml document to output xml
	 * blocks as records as specified by the start tag and end tag
	 *
	 */
	private static class KCVRecordReader extends RecordReader<LongWritable, Text> {
		private Pattern headerLinePattern;
		private Pattern footerLinePattern;
		private long start;
		private long end;
		private FSDataInputStream fsin;
		private InputStreamReader streamReader;
		private BufferedReader in;
		private DataOutputBuffer buffer;
		private DataOutputBuffer bufferHeader;

		private LongWritable key = new LongWritable();
		private Text value = new Text();

		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			if (conf.get(HEADER_LINE_KEY) != null && conf.get(HEADER_LINE_KEY).length() > 0)
				headerLinePattern = Pattern.compile(conf.get(HEADER_LINE_KEY));
			if (conf.get(FOOTER_LINE_KEY) != null && conf.get(FOOTER_LINE_KEY).length() > 0)
				footerLinePattern = Pattern.compile(conf.get(FOOTER_LINE_KEY));
			FileSplit fileSplit = (FileSplit) split;

			// open the file and seek to the start of the split
			start = fileSplit.getStart();
			end = start + fileSplit.getLength();
			Path file = fileSplit.getPath();
			FileSystem fs = file.getFileSystem(conf);
			fsin = fs.open(fileSplit.getPath());
			fsin.seek(start);
			
			streamReader = new InputStreamReader(fsin);

			in = new BufferedReader(streamReader);
			
			buffer = new DataOutputBuffer();
			
			if (footerLinePattern == null) {
				bufferHeader = new DataOutputBuffer();
				/* Read the first header line */
				readUntilMatch(headerLinePattern);
			}
		}

		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (fsin.getPos() < end) {
				if (readUntilMatch(headerLinePattern)) {
					try {
						if (footerLinePattern != null) {
							if (readUntilMatch(footerLinePattern)) {
								key.set(fsin.getPos());
								value.set(buffer.getData(), 0, buffer.getLength());
								return true;
							}
						}
						
						key.set(fsin.getPos());
						value.set(bufferHeader.getData(), 0, bufferHeader.getLength());
						value.append(buffer.getData(), 0, buffer.getLength());
						return true;
					} finally {
						if (bufferHeader != null)
							bufferHeader.reset();
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
			if (bufferHeader != null) {
				bufferHeader.close();
				bufferHeader = null;
			}
			if (buffer != null) {
				buffer.close();
				buffer = null;
			}
			if (in != null) {
				in.close();
				in = null;
			}
			if (streamReader != null) {
				streamReader.close();
				streamReader = null;
			}
			if (fsin != null) {
				fsin.close();
				fsin = null;
			}
		}

		public float getProgress() throws IOException {
			return (fsin.getPos() - start) / (float) (end - start);
		}

		private boolean readUntilMatch(Pattern match) throws IOException {
			String line;
			while ((line = in.readLine()) != null) {
				Matcher matcher = match.matcher(line);
				if (matcher.find()) {
					if (bufferHeader != null) {
						bufferHeader.write(line.getBytes(StandardCharsets.UTF_8));
						bufferHeader.write("\n".getBytes(StandardCharsets.UTF_8));
					} else {
						buffer.write(line.getBytes(StandardCharsets.UTF_8));
						buffer.write("\n".getBytes(StandardCharsets.UTF_8));
					}
					return true;
				} else {
					// save to buffer:
					buffer.write(line.getBytes(StandardCharsets.UTF_8));
					buffer.write("\n".getBytes(StandardCharsets.UTF_8));
				}
			}
				
			return false;
		}
	}
}
