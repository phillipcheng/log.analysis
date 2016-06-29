package etl.cmd.test;

import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.junit.Test;

import etl.cmd.ShellCmd;
import etl.util.Util;



public class TestShellCmd extends TestETLCmd {
	public static final Logger logger = Logger.getLogger(ShellCmd.class);
	@Test
	public void testShellCmd() throws Exception {
		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
	    ugi.doAs(new PrivilegedExceptionAction<Void>() {
	      public Void run() throws Exception {
	       	Configuration conf = new Configuration();
	    	String defaultFS = "hdfs://192.85.247.104:19000";
			conf.set("fs.defaultFS", defaultFS);
			FileSystem fs = FileSystem.get(conf);
	    	String dfsFolder = "/pde/etlcfg/";
	    	String staticCfgName = "shellCmd.properties";
	    //	String csvFile="PJ24002B_BBG2.csv";
			String wfid="wfid1";
			String prefix = "sgsiwf";
			fs.copyFromLocalFile(new Path(getLocalFolder() + staticCfgName), new Path(dfsFolder+staticCfgName));
			ShellCmd cmd = new ShellCmd(null, dfsFolder+staticCfgName, null, null, defaultFS);
			cmd.process(0,"PK020000_BBG2.bin", null);
			return null;
	  	}
	    });
	}
}
