package etl.cmd.test;

import java.util.List;

import org.apache.hadoop.fs.Path;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class TestMapToCsvCmd extends TestETLCmd {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3432808041331849272L;
	public static final Logger logger = LogManager.getLogger(TestMapToCsvCmd.class);
	public static final String testCmdClass = "etl.cmd.MapToCsvCmd";

	public String getResourceSubFolder(){
		return "mapToCsv/";
	}
	
	@Test
	public void testMR() throws Exception {
		String inputFolder = "/etltest/deltaloadcmd/input/";
		String outputFolder = "/etltest/deltaloadcmd/output/";
		String cfgFolder = "/etltest/deltaloadcmd/cfg/";
		String dataFolder= "/etltest/deltaloadcmd/data/";
		
		String csvtransProp = "conf.properties";
		String schemaFileName = "spc.schema";
		
		String[] csvFiles = new String[] {"SPC_TOKENS_20170207133000000.txt"};
		
		getFs().delete(new Path(dataFolder), true);
		getFs().mkdirs(new Path(dataFolder));
		
		getFs().delete(new Path(outputFolder), true);
		getFs().mkdirs(new Path(outputFolder));
		
		getFs().delete(new Path(cfgFolder), true);
		getFs().mkdirs(new Path(cfgFolder));
		
		//copy schema file
		getFs().copyFromLocalFile(new Path(getLocalFolder() + schemaFileName), new Path(cfgFolder + schemaFileName));
		
		List<String> output = super.mrTest(inputFolder, outputFolder, csvtransProp, csvFiles, testCmdClass, false);
		logger.info("Output is:\n{}",String.join("\n", output));
		Assert.assertEquals(11,output.size());
		Assert.assertTrue(output.contains("SPC_IMS_PRIDS,2017-03-23 22:51:13,Update,2017-03-23,22:51:13,9999999123,blank,blank,blank,VCE,2000014,,,"));
		Assert.assertTrue(output.contains("SPC_IMS_PRIDS,2017-03-23 22:51:10,Insert,2017-03-23,22:51:10,9999999123,blank,dev9123_s1,,V4B,2000014,,,"));
		
	}
}
