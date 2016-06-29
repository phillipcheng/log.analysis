package etl.cmd.test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.junit.Test;

import etl.cmd.SftpCmd;
import etl.util.Util;

public class TestSftpCmd extends TestETLCmd {
	public static final Logger logger = Logger.getLogger(TestSftpCmd.class);

	@Test
	public void testSuccess() throws Exception {
		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				String dfsCfg = "/test/sftpcmd/cfg/";
				String cfg = "sftp_test.properties";
				// values in the static cfg
				String incomingFolder = "/test/sftp/incoming/";
				String ftpFolder = "/data/mtccore/sftptest/";
				String host = "192.85.247.104";
				int port = 22;
				String user = "dbadmin";
				String pass = "password";
				String fileName = "backup_test1_data";
				List<String> fileNames = new ArrayList<String>(Arrays.asList(fileName));
				//
				getFs().copyFromLocalFile(new Path(getLocalFolder() + cfg), new Path(dfsCfg + cfg));
				Util.sftpFromLocal(host, port, user, pass, getLocalFolder() + fileName, ftpFolder + fileName);
				//
				getFs().delete(new Path(incomingFolder+fileName),false);
				Util.deleteFiles(incomingFolder,fileNames);
				SftpCmd cmd = new SftpCmd(null, dfsCfg + cfg, null, null, getDefaultFS());
				cmd.process(0, String.format("sftp.host=%s, sftp.folder=%s, sftp.clean=true", host, ftpFolder), null);
				// check incoming fodler
				List<String> fl = Util.listDfsFile(getFs(), incomingFolder);
				assertTrue(fl.contains(fileName));
				// check remote dir
				fl = Util.sftpList(host, port, user, pass, ftpFolder);
				assertFalse(fl.contains(fileName));

				return null;
			}
		});
	}


	@Test
	public void testCommonFailure() throws Exception {
		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				logger.info("Testing sftpcmd common failure test case");
				Configuration conf = new Configuration();
				String defaultFS = "hdfs://192.85.247.104:19000";
				String ftpFolder = "/data/mtccore/source1/";
				String incomingFolder = "/data/mtccore/test/";
				String host = "192.85.247.104";
				int port = 22;
				String user = "dbadmin";
				String pass = "password";
				String fileName = "backup_test1_data";
				conf.set("fs.defaultFS", defaultFS);
				FileSystem fs = FileSystem.get(conf);
				String dfsFolder = "/test/sftpcmd/cfg/";
				String cfg = "sftp_test.properties";
				fs.copyFromLocalFile(new Path(getLocalFolder() + cfg), new Path(dfsFolder + cfg));				
				getFs().delete(new Path(incomingFolder+fileName),false);
				
				SftpCmd cmd = new SftpCmd(null, dfsFolder + cfg, null, null, defaultFS);
				cmd.process(0, String.format("sftp.host=%s, sftp.folder=%s, sftp.clean=true", host, ftpFolder), null);

				// check incoming fodler
				List<String> fl = Util.listDfsFile(getFs(), incomingFolder);
				assertFalse(fl.contains(fileName));

				return null;
			}
		});
	}

	@Test
	public void testConnectionFailure() throws Exception {
		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				logger.info("Testing sftpcmd connection failure with wrong sftp port number");
				Configuration conf = new Configuration();
				String defaultFS = "hdfs://192.85.247.104:19000";
				String ftpFolder = "/data/mtccore/source/";
				String incomingFolder = "/data/mtccore/test/";
				String host = "192.85.247.104";
				int port = 25;
				String user = "dbadmin";
				String pass = "password";
				String fileName = "backup_test1_data";
				conf.set("fs.defaultFS", defaultFS);
				FileSystem fs = FileSystem.get(conf);
				String dfsFolder = "/test/sftpcmd/cfg/";
				String cfg = "sftp_test.properties";
				//
				fs.copyFromLocalFile(new Path(getLocalFolder() + cfg), new Path(dfsFolder + cfg));
				getFs().delete(new Path(incomingFolder+fileName),false);
				SftpCmd cmd = new SftpCmd(null, dfsFolder + cfg, null, null, defaultFS);
				cmd.process(0, String.format("sftp.host=%s, sftp.folder=%s, sftp.port=%s", host, ftpFolder, port),
						null);

				// check incoming fodler
				List<String> fl = Util.listDfsFile(getFs(), incomingFolder);
				assertFalse(fl.contains(fileName));

				return null;
			}
		});
	}

	@Test
	public void testDeleteFileNotEnabled() throws Exception {
		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				logger.info("Testing sftpcmd delete not enabled test case");

				String dfsCfg = "/test/sftpcmd/cfg/";
				String cfg = "sftp_test.properties";
				// values in the static cfg
				String incomingFolder = "/test/sftp/incoming/";
				String ftpFolder = "/data/mtccore/sftptest/";
				String host = "192.85.247.104";
				int port = 22;
				String user = "dbadmin";
				String pass = "password";
				String sftpClean = "false";
				String fileName = "backup_test1_data";
				//
				getFs().copyFromLocalFile(new Path(getLocalFolder() + cfg), new Path(dfsCfg + cfg));
				Util.sftpFromLocal(host, port, user, pass, getLocalFolder() + fileName, ftpFolder + fileName);
				getFs().delete(new Path(incomingFolder+fileName),false);
				//
				SftpCmd cmd = new SftpCmd(null, dfsCfg + cfg, null, null, getDefaultFS());
				cmd.process(0, String.format("sftp.host=%s, sftp.folder=%s, sftp.clean=%s", host, ftpFolder, sftpClean),
						null);
				// check incoming fodler
				List<String> fl = Util.listDfsFile(getFs(), incomingFolder);
				assertTrue(fl.contains(fileName));
				// check remote dir
				fl = Util.sftpList(host, port, user, pass, ftpFolder);
				assertTrue(fl.contains(fileName));

				return null;

			}
		});
	}

}
