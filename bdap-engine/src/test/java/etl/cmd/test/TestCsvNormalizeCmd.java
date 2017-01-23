package etl.cmd.test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import bdap.util.HdfsUtil;
import etl.output.ParquetOutputFormat;
import scala.Tuple2;

public class TestCsvNormalizeCmd extends TestETLCmd {
	private static final long serialVersionUID = 1L;
	public static final Logger logger = LogManager.getLogger(TestCsvNormalizeCmd.class);
	public static final String testCmdClass="etl.cmd.CsvNormalizeCmd";

	public String getResourceSubFolder(){
		return "csvnorm/";
	}
	
	@Test
	public void testCsvNormalize() throws Exception {
		String remoteCsvFolder = "/etltest/csvnorminput/";
		String remoteCsvOutputFolder = "/etltest/csvnormoutput/";
		String csvsplitProp = "csvnorm.properties";
		String[] csvFiles = new String[] {"MyCore_-r-00100.csv"};

		String schemaFolder="/etltest/csvnorm/schema/";
		String localSchemaFile = "schema-index.schema";
		String localSchemaFile2 = "MyCore_.schema";
		String remoteSchemaFile = "schema-index.schema";
		String remoteSchemaFile2 = "MyCore_.schema";

		//schema
		getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + localSchemaFile), new Path(schemaFolder + remoteSchemaFile));
		getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + localSchemaFile2), new Path(schemaFolder + remoteSchemaFile2));
		
		List<Tuple2<String, String[]>> rfifs = new ArrayList<Tuple2<String, String[]>>();
		rfifs.add(new Tuple2<String, String[]>(remoteCsvFolder, csvFiles));
		getConf().set("logic.schema", "/etltest/csvnorm/schema");
		List<String> output = mrTest(rfifs, remoteCsvOutputFolder, csvsplitProp, testCmdClass, TextInputFormat.class, ParquetOutputFormat.class);
		logger.info("Output is: {}", output);
		
		// assertion
		assertTrue(output.size() > 0);
		assertTrue(output.contains("2016-03-28 11:05:00,PT300S,QDSD0101vSGS-L-NK-20,lcp-1,QDSD0101vSGS-L-NK-20-VLR-00,1,b69231ce-8926-4e88-87cc-93fbb46d70e9,57364,57364,0,0"));
		
		List<String> files = HdfsUtil.listDfsFile(getFs(), remoteCsvOutputFolder);
		logger.info("Output files: {}", files);
		Predicate predicate = new Predicate() {
			public boolean evaluate(Object object) {
				return object != null && object.toString().endsWith(".parquet");
			}
		};
		CollectionUtils.filter(files, predicate);
		assertEquals(files.size(), 2);
	}
}
