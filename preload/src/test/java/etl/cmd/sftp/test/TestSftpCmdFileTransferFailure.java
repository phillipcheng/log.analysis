package etl.cmd.sftp.test;

import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.junit.Test;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.Session;

import etl.cmd.SftpCmd;
import etl.cmd.test.TestETLCmd;

public class TestSftpCmdFileTransferFailure extends TestETLCmd{
	@Test
	public void test1() throws Exception {
		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
	    ugi.doAs(new PrivilegedExceptionAction<Void>() {
	      public Void run() throws Exception {
	    	  Logger logger = Logger.getLogger(TestSftpCmdConnectionFailure.class);

				Session session = null;
				ChannelSftp sftpChannel = null;
	    	Configuration conf = new Configuration();
	    	String defaultFS = "hdfs://192.85.247.104:19000";
			conf.set("fs.defaultFS", defaultFS);
			FileSystem fs = FileSystem.get(conf);
			String dfsFolder = "/data/sample/";
	    	String cfg = "sftp_test.properties";
			fs.copyFromLocalFile(new Path(getLocalFolder() + cfg), new Path(dfsFolder+cfg));
			SftpCmd cmd = new SftpCmd(null, dfsFolder+cfg, null, null, defaultFS);
			cmd.process(0, "sftp.host=192.85.247.104, sftp.folder=/data/mtccore/source/", null);
			return null;   

	  	}
	    });
	}
}
