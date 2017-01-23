package etl.output;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import etl.util.CSVWriteSupport;

public class ParquetOutputFormat extends org.apache.parquet.hadoop.ParquetOutputFormat<Text> {
	private static final Logger logger = LogManager.getLogger(ParquetOutputFormat.class);
	
	public ParquetOutputFormat() {
		super(new CSVWriteSupport());
	}

	public RecordWriter<Void, Text> getRecordWriter(Configuration conf, Path file, CompressionCodecName codec)
			throws IOException, InterruptedException {
		WriteSupport<Text> s = getWriteSupport(conf);
		if (s instanceof CSVWriteSupport) {
			logger.debug("Write the file: {}", file);
			((CSVWriteSupport)s).initTableName(conf, file);
		}
		RecordWriter<Void, Text> w = super.getRecordWriter(conf, file, codec);
		return w;
	}
}
