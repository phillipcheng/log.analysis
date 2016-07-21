package ems.preload.test;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import etl.cmd.test.TestETLCmd;

public class EradETLCmd extends TestETLCmd {
	
	@Test
	public void setupLabETLCfg() throws Exception {
		/*
		super.setupWorkflow("/user/dbadmin/atterad", "/atterad/etlcfg", 
				"C:\\mydoc\\myprojects\\log.analysis\\ems_analysis\\target\\", "ems-0.0.1-jar-with-dependencies.jar", 
				"C:\\mydoc\\myprojects\\log.analysis\\ems_analysis\\lib\\", "vertica-jdbc-7.0.1-0.jar");
		*/
		getFs().delete(new Path("/atterad/input2"), true);
		getFs().delete(new Path("/atterad/input3"), true);
	}
}
