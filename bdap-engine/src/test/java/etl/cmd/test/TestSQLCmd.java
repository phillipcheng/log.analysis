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

public class TestSQLCmd extends TestETLCmd {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3432808041331849272L;
	public static final Logger logger = LogManager.getLogger(TestSQLCmd.class);
	public static final String testCmdClass = "etl.cmd.SQLCmd";

	public String getResourceSubFolder(){
		return "sqlcmd/";
	}
	
	@Test
	public void testMR() throws Exception {
		String inputFolder = "/etltest/sqlcmd/input/";
		String outputFolder = "/etltest/sqlcmd/output/";
		String cfgFolder = "/etltest/sqlcmd/cfg/";
		String dataFolder= "/etltest/sqlcmd/data/";
		
		String csvtransProp = "sqlcmd.properties";
		
		String[] csvFiles = new String[] {"sqls.properties"};
		
		getFs().delete(new Path(dataFolder), true);
		getFs().mkdirs(new Path(dataFolder));
		
		getFs().delete(new Path(outputFolder), true);
		getFs().mkdirs(new Path(outputFolder));
		
		getFs().delete(new Path(cfgFolder), true);
		getFs().mkdirs(new Path(cfgFolder));
		
		List<String> output = super.mrTest(inputFolder, outputFolder, csvtransProp, csvFiles, testCmdClass, false);
		logger.info("Output is:\n{}",String.join("\n", output));
		Assert.assertEquals(4,output.size());
	}
}
