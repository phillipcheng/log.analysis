package etl.cmd.test;

import java.util.List;

import org.apache.hadoop.fs.Path;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class TestIncrementalLoadCmd extends TestETLCmd {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3432808041331849272L;
	public static final Logger logger = LogManager.getLogger(TestIncrementalLoadCmd.class);
	public static final String testCmdClass = "etl.cmd.IncrementalLoadCmd";

	public String getResourceSubFolder(){
		return "incrementalLoad/";
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
		Assert.assertEquals(3,output.size());
		Assert.assertTrue(output.contains("SPC_TOKENS,2017-03-17 18:09:56,Update,2017-03-17,18:09:56,9728375612,1,AE,EAP,EAP,1,2017-03-17 18:09:56,1489774196220,blank,,grdev1,1,1489774196217,blank"));
		Assert.assertTrue(output.contains("SPC_TOKENS,2017-03-17 18:09:56,Update,2017-03-17,18:09:56,9728375612,1,AE,EAP,EAP,1,2017-03-17 18:09:56,1489774196220,blank,1,grdev1,1,1489774196217,blank"));
		Assert.assertTrue(output.contains("SPC_TOKENS,2017-03-17 18:09:56,Insert,2017-03-17,18:09:56,9728375612,1,AE,EAP,EAP,1,2017-03-17 18:09:56,1489774196212,blank,,grdev1,1,1489774196217,blank"));
		
	}
}
