package etl.cmd.test;

import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.Test;

import etl.cmd.transform.KcvToCsvCmd;

public class TestKcvToCsvTransform extends TestETLCmd{
	public static final Logger logger = Logger.getLogger(TestKcvToCsvTransform.class);
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	@Test
	public void test1(){
		try{
			String remoteCfgFolder = "/etltest/cfg/";
			String remoteCsvFolder = "/etltest/kcvtransform/";
			String remoteCsvOutputFolder = "/etltest/kcvtransformout/";
			//setup testing env
			String csvtransProp = "kcv2csv.properties";
			String kcvFile = "PJ24002A_BBG2.fix";
			getFs().mkdirs(new Path(remoteCfgFolder));
			getFs().mkdirs(new Path(remoteCsvFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder()+csvtransProp), new Path(remoteCfgFolder+csvtransProp));
			getFs().copyFromLocalFile(new Path(getLocalFolder()+kcvFile), new Path(remoteCsvFolder+kcvFile));
			getFs().delete(new Path(remoteCsvOutputFolder), true);
			//run job
			KcvToCsvCmd cmd = new KcvToCsvCmd(null, remoteCfgFolder+csvtransProp, null, null, getDefaultFS());
			List<String> retlist = cmd.process(0, kcvFile, null);
			logger.info(retlist);
		} catch (Exception e) {
			logger.error("", e);
		}
	}
}
