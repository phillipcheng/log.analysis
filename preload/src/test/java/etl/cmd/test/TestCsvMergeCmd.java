package etl.cmd.test;

import static org.junit.Assert.*;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.junit.Test;

import scala.Tuple2;

public class TestCsvMergeCmd extends TestETLCmd {
	public static final Logger logger = Logger.getLogger(TestCsvMergeCmd.class);
	public static final String testCmdClass="etl.cmd.CsvMergeCmd";
	public String getResourceSubFolder(){
		return "csvmerge/";
	}
	
	private void innerMerge() throws Exception {
		try {
			String remoteCfgFolder = "/etltest/cfg/";
			String remoteCsvOutputFolder = "/etltest/csvmergeout/";
			String prop = "csvmerge1.properties";
			List<Tuple2<String, String[]>> ififs = new ArrayList<Tuple2<String, String[]>>();
			ififs.add(new Tuple2<String, String[]>("/test/a/", new String[]{"csv.txt"}));
			ififs.add(new Tuple2<String, String[]>("/test/b/", new String[]{"fix.csv"}));
			List<String> output = super.mrTest(remoteCfgFolder, ififs, remoteCsvOutputFolder, prop, testCmdClass, false);
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
	
	@Test
	public void testInnerMerge() throws Exception {
		if (getDefaultFS().contains("127.0.0.1")){
			innerMerge();
		}else{
			UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					innerMerge();
					return null;
				}
			});
		}
	}
}
