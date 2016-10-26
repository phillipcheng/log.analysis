package etl.cmd.test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import scala.Tuple2;

public class TestCsvMergeCmd extends TestETLCmd {
	public static final Logger logger = LogManager.getLogger(TestCsvMergeCmd.class);
	public static final String testCmdClass="etl.cmd.CsvMergeCmd";
	public String getResourceSubFolder(){
		return "csvmerge/";
	}
	
	@Test
	public void testInnerMerge() throws Exception {
		try {
			String remoteCsvOutputFolder = "/etltest/csvmergeout/";
			String prop = "csvmerge1.properties";
			List<Tuple2<String, String[]>> ififs = new ArrayList<Tuple2<String, String[]>>();
			ififs.add(new Tuple2<String, String[]>("/test/a/", new String[]{"csv.txt"}));
			ififs.add(new Tuple2<String, String[]>("/test/b/", new String[]{"fix.csv"}));
			List<String> output = super.mrTest(ififs, remoteCsvOutputFolder, prop, testCmdClass, false);
			logger.info("Output is:"+ String.join("\n", output));
			
			// assertion
			assertTrue(output.size() ==3);
			String sampleOutput = output.get(0);
			String[] csvs = sampleOutput.split(",", -1);
			assertTrue("393230".equals(csvs[csvs.length-1].trim()));
			
		} catch (Exception e) {
			logger.error("", e);
		}
	}
}
