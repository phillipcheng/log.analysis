package hpe.pde.test;

import static org.junit.Assert.assertTrue;

import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.Test;

import etl.cmd.test.TestETLCmd;
import etl.util.Util;

public class TestSesToCsvCmd extends TestETLCmd{
	public static final Logger logger = Logger.getLogger(TestSesToCsvCmd.class);
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	
	public static String cmdClassName ="hpe.pde.cmd.SesToCsvCmd";
	@Test
	public void test1(){
		try{
			String remoteCfgFolder = "/etltest/cfg/";
			String remoteSesFolder = "/etltest/sestransform/";
			String remoteCsvOutputFolder = "/etltest/sestransformoutput/";
			//setup testing env
			String csvtransProp = "pde.ses2csv.properties";
			String[] sesFiles = new String[]{"PK020000_BBG2.ses"};
			
			super.mapTest(remoteCfgFolder, remoteSesFolder, remoteCsvOutputFolder, csvtransProp, sesFiles, cmdClassName, true);
			
			//assertion
			List<String> output = Util.getMROutput(getFs(), remoteCsvOutputFolder);
			assertTrue(output.size()>0);
			String sampleOutput = output.get(0);
			String[] csvs = sampleOutput.split(",");
			logger.info(output);
			assertTrue("BBG2".equals(csvs[csvs.length-1])); //check filename appended to last
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	@Override
	public String getResourceSubFolder() {
		// TODO Auto-generated method stub
		return null;
	}
}
