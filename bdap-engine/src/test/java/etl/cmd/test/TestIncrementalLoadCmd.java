package etl.cmd.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import scala.Tuple2;

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
		Assert.assertEquals(5,output.size());
		Assert.assertTrue(output.contains("INSERT INTO spctest.SPC_TOKENS (SPC_MDN,SPC_TOKENSTATUS,SPC_SUBSTOKEN ) VALUES ( '9728375612',1,'1');"));
		Assert.assertTrue(output.contains("UPDATE spctest.SPC_TOKENS SET SPC_TOKENSTATUS=2,SPC_SUBSTOKEN='1' WHERE SPC_MDN='9728375612';"));
		Assert.assertTrue(output.contains("UPDATE spctest.SPC_TOKENS SET SPC_TOKENSTATUS=3 WHERE SPC_MDN='9728375612';"));
		Assert.assertTrue(output.contains("UPDATE spctest.SPC_TOKENS SET SPC_TOKENSTATUS=2,SPC_DATE='2012-01-01',SPC_TIME='2012-01-01' WHERE SPC_MDN='9728375612';"));
		Assert.assertTrue(output.contains("DELETE FROM spctest.SPC_TOKENS WHERE SPC_MDN='9728375612';"));
		
	}
}
