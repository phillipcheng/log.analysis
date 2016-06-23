package etl.cmd.test;

import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.junit.Test;

import etl.cmd.SftpCmd;

public class TestSftpCmdConnectionFailure {
	public static final Logger logger = Logger.getLogger(TestSftpCmdConnectionFailure.class);
	@Test
	public void test1() throws Exception {
		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
	    ugi.doAs(new PrivilegedExceptionAction<Void>() {
	      public Void run() throws Exception {
	    	logger.info("Testing sftpcmd connection failure with wrong sftp port number");
	    	Configuration conf = new Configuration();
	    	String defaultFS = "hdfs://192.85.247.104:19000";
			conf.set("fs.defaultFS", defaultFS);
			FileSystem fs = FileSystem.get(conf);
			String localFolder = "C:\\Users\\rangasap\\workspace\\log.analysis\\preload\\src\\test\\resources\\";
	    	String dfsFolder = "/data/sample/";
	    	String cfg = "sftp_test.properties";
			fs.copyFromLocalFile(new Path(localFolder + cfg), new Path(dfsFolder+cfg));
			SftpCmd cmd = new SftpCmd(null, dfsFolder+cfg, null, null, defaultFS);
			cmd.process("sftp.host=192.85.247.104, sftp.folder=/data/mtccore/source/, sftp.port=25", null);
			return null;   

	  	}
	    });
	}
}
