package etl.cmd.test;

import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import etl.cmd.SftpCmd;

public class TestSftpCmd {
	
	@Test
	public void test1() throws Exception {
		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
	    ugi.doAs(new PrivilegedExceptionAction<Void>() {
	      public Void run() throws Exception {
	    	Configuration conf = new Configuration();
	    	String defaultFS = "hdfs://192.85.247.104:19000";
			conf.set("fs.defaultFS", defaultFS);
			FileSystem fs = FileSystem.get(conf);
			String localFolder = "C:\\mydoc\\myprojects\\log.analysis\\preload\\src\\test\\resources\\";
	    	String dfsFolder = "/mtccore/etlcfg/";
	    	String cfg = "sftp.properties";
			fs.copyFromLocalFile(new Path(localFolder + cfg), new Path(dfsFolder+cfg));
			SftpCmd cmd = new SftpCmd(null, dfsFolder+cfg, null, null, defaultFS);
			cmd.process(0, "sftp.host=192.85.247.104, sftp.folder=/data/log.analysis/bin/", null);
			return null;
	      }
	    });
	}
}
