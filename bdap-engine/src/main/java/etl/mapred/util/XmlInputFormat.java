package etl.mapred.util;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

import etl.mapred.util.XmlRecordReader;

public class XmlInputFormat extends TextInputFormat {

	@Override
	protected boolean isSplitable(FileSystem fs, Path file) {
		return false;
	}
	
	@Override
	public RecordReader<LongWritable, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter){
		return new XmlRecordReader(split, job, reporter);
	}
	
}
