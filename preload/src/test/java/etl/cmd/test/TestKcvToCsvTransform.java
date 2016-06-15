package etl.cmd.test;

import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.Test;

import etl.cmd.transform.KcvToCsvCmd;

public class TestKcvToCsvTransform {
	public static final Logger logger = Logger.getLogger(TestKcvToCsvTransform.class);
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	@Test
	public void test1(){
		try{
			Configuration conf = new Configuration();
			String defaultFS = "hdfs://127.0.0.1:19000";
			conf.set("fs.defaultFS", defaultFS);
			String remoteCfgFolder = "/etltest/cfg/";
			String remoteCsvFolder = "/etltest/kcvtransform/";
			String remoteCsvOutputFolder = "/etltest/kcvtransformout/";
			FileSystem fs = FileSystem.get(conf);
			//setup testing env
			String localResourceFolder = "/Users/chengyi/git/log.analysis/preload/src/test/resources/";
			String csvtransProp = "kcv2csv.properties";
			String kcvFile = "PJ24002A_BBG2.fix";
			fs.mkdirs(new Path(remoteCfgFolder));
			fs.mkdirs(new Path(remoteCsvFolder));
			fs.copyFromLocalFile(new Path(localResourceFolder+csvtransProp), new Path(remoteCfgFolder+csvtransProp));
			fs.copyFromLocalFile(new Path(localResourceFolder+kcvFile), new Path(remoteCsvFolder+kcvFile));
			fs.delete(new Path(remoteCsvOutputFolder), true);
			//run job
			KcvToCsvCmd cmd = new KcvToCsvCmd(null, remoteCfgFolder+csvtransProp, null, null, defaultFS);
			List<String> retlist = cmd.process(kcvFile, null);
			logger.info(retlist);
		} catch (Exception e) {
			logger.error("", e);
		}
	}
}
