package hpe.pde.test;

import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.Test;

import hpe.pde.cmd.SesToCsvCmd;

public class TestSesToCsvCmd {
	public static final Logger logger = Logger.getLogger(TestSesToCsvCmd.class);
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	@Test
	public void test1(){
		try{
			Configuration conf = new Configuration();
			String defaultFS = "hdfs://127.0.0.1:19000";
			conf.set("fs.defaultFS", defaultFS);
			String remoteCfgFolder = "/etltest/cfg/";
			String remoteSesFolder = "/etltest/sestransform/";
			FileSystem fs = FileSystem.get(conf);
			//setup testing env
			String localResourceFolder = "C:\\mydoc\\myprojects\\log.analysis\\pde.analysis\\src\\test\\resources\\";
			String csvtransProp = "pde.ses2csv.properties";
			String sesFile = "PK020000_BBG2.ses";
			fs.mkdirs(new Path(remoteCfgFolder));
			fs.mkdirs(new Path(remoteSesFolder));
			fs.copyFromLocalFile(new Path(localResourceFolder+csvtransProp), new Path(remoteCfgFolder+csvtransProp));
			fs.copyFromLocalFile(new Path(localResourceFolder+sesFile), new Path(remoteSesFolder+sesFile));
			//run job
			SesToCsvCmd cmd = new SesToCsvCmd(null, remoteCfgFolder+csvtransProp, null, null, defaultFS);
			List<String> retlist = cmd.process(0, sesFile, null);
			logger.info(retlist);
		} catch (Exception e) {
			logger.error("", e);
		}
	}
}
