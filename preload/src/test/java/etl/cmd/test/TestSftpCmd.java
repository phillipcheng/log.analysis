package etl.cmd.test;

import static org.junit.Assert.*;

import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.junit.Test;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.ChannelSftp.LsEntry;

import etl.cmd.SftpCmd;
import etl.util.Util;

public class TestSftpCmd extends TestETLCmd{
	public static final Logger logger = Logger.getLogger(TestSftpCmd.class);
	
	@Test
	public void test1() throws Exception{
    	String dfsCfg = "/test/sftpcmd/cfg/";
    	String cfg = "sftp.properties";
    	//values in the static cfg
    	String incomingFolder = "/test/sftp/incoming/";
    	String ftpFolder = "/data/mtccore/sftptest/";
    	String host = "192.85.247.104";
    	int port = 22;
    	String user = "dbadmin";
    	String pass = "password";
    	String fileName = "backup_test1_data";
		//
    	getFs().copyFromLocalFile(new Path(getLocalFolder() + cfg), new Path(dfsCfg+cfg));
		Util.sftpFromLocal(host, port, user, pass, getLocalFolder()+fileName, ftpFolder+fileName);
		//
		SftpCmd cmd = new SftpCmd(null, dfsCfg+cfg, null, null, getDefaultFS());
		cmd.process(0, String.format("sftp.host=%s, sftp.folder=%s, sftp.clean=true", host, ftpFolder), null);
		//check incoming fodler
		List<String> fl = Util.listDfsFile(getFs(), incomingFolder);
		assertTrue(fl.contains(fileName));
		//check remote dir
		fl = Util.sftpList(host, port, user, pass, ftpFolder);
		assertFalse(fl.contains(fileName));
	}
	
	@Test
	public void remoteTest1() throws Exception {
		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
	    ugi.doAs(new PrivilegedExceptionAction<Void>() {
	      public Void run() throws Exception {
	    	test1();
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
			conf.set("fs.defaultFS", defaultFS);
			FileSystem fs = FileSystem.get(conf);
			String dfsFolder = "/data/sample/";
	    	String cfg = "sftp_test.properties";
			fs.copyFromLocalFile(new Path(getLocalFolder() + cfg), new Path(dfsFolder+cfg));
			SftpCmd cmd = new SftpCmd(null, dfsFolder+cfg, null, null, defaultFS);
			cmd.process(0, "sftp.host=192.85.247.104, sftp.folder=/data/mtccore/source1/", null);
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
			conf.set("fs.defaultFS", defaultFS);
			FileSystem fs = FileSystem.get(conf);
	    	String dfsFolder = "/data/sample/";
	    	String cfg = "sftp_test.properties";
			fs.copyFromLocalFile(new Path(getLocalFolder() + cfg), new Path(dfsFolder+cfg));
			SftpCmd cmd = new SftpCmd(null, dfsFolder+cfg, null, null, defaultFS);
			cmd.process(0, "sftp.host=192.85.247.104, sftp.folder=/data/mtccore/source/, sftp.port=25", null);
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
	    	Configuration conf = new Configuration();
	    	String defaultFS = "hdfs://192.85.247.104:19000";
			conf.set("fs.defaultFS", defaultFS);
			FileSystem fs = FileSystem.get(conf);
			Session session = null;
			ChannelSftp sftpChannel = null;
			String dfsFolder = "/data/sample/";
	    	String cfg = "sftp_test.properties";
			fs.copyFromLocalFile(new Path(getLocalFolder() + cfg), new Path(dfsFolder+cfg));
			
			
			logger.info("Before executing sftp command");
			JSch jsch = new JSch();
			Channel channel = null;
			session = jsch.getSession("dbadmin", "192.85.247.104", 22);
			session.setConfig("StrictHostKeyChecking", "no");
			session.setPassword("password");
			session.connect();
			channel = session.openChannel("sftp");
			channel.connect();
			sftpChannel = (ChannelSftp) channel;
			logger.info("Files in source directory");
			sftpChannel.cd("/data/mtccore/source/");
			Vector<LsEntry> v = sftpChannel.ls("*");
			for (LsEntry entry : v) {
				logger.info(entry.getFilename());
			}
			fs.delete(new Path("/data/mtccore/test/sample.txt"));
			fs.delete(new Path("/data/mtccore/test/sample1.txt"));

			
			SftpCmd cmd = new SftpCmd(null, dfsFolder+cfg, null, null, defaultFS);
			cmd.process(0, "sftp.host=192.85.247.104, sftp.folder=/data/mtccore/source/, sftp.clean=false", null);
			logger.info("Executed sftpcmd delete not enabled test case");
			
			
			logger.info("After executing sftp command");				
			logger.info("Files in source directory");
			sftpChannel.cd("/data/mtccore/source/");
			v = sftpChannel.ls("*");
			logger.info("Files present in source folder:");
			for (LsEntry entry : v) {
				logger.info(entry.getFilename());
			}	
			logger.info("Files in destination directory:");
			FileStatus[] files = fs.listStatus(new Path("/data/mtccore/test"));
			for (FileStatus fileStatus : files) {
				 String fileName = fileStatus.getPath().toString(); 
				 logger.info(fileName.substring(fileName.lastIndexOf("/") + 1));
			}
			return null;
	  	}
	    });
	}
	
	@Test
	public void testFileTransferFailure() throws Exception {
		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
	    ugi.doAs(new PrivilegedExceptionAction<Void>() {
	      public Void run() throws Exception {
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
	
	@Test
	public void testSuccess() throws Exception {
		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				Session session = null;
				ChannelSftp sftpChannel = null;
				Configuration conf = new Configuration();
				String defaultFS = "hdfs://192.85.247.104:19000";
				conf.set("fs.defaultFS", defaultFS);
				FileSystem fs = FileSystem.get(conf);
				String dfsFolder = "/data/sample/";
				String cfg = "sftp_test.properties";
				fs.copyFromLocalFile(new Path(getLocalFolder() + cfg), new Path(dfsFolder + cfg));
				
				logger.info("Before executing sftp command");
				JSch jsch = new JSch();
				Channel channel = null;
				session = jsch.getSession("dbadmin", "192.85.247.104", 22);
				session.setConfig("StrictHostKeyChecking", "no");
				session.setPassword("password");
				session.connect();
				channel = session.openChannel("sftp");
				channel.connect();
				sftpChannel = (ChannelSftp) channel;
				sftpChannel.cd("/data/mtccore/source/");
				Vector<LsEntry> v = sftpChannel.ls("*");
				logger.info("Files present in source folder:");
				for (LsEntry entry : v) {
					logger.info(entry.getFilename());
				}
				fs.delete(new Path("/data/mtccore/test/sample.txt"));
				fs.delete(new Path("/data/mtccore/test/sample1.txt"));
				
				
				SftpCmd cmd = new SftpCmd(null, dfsFolder + cfg, null, null, defaultFS);
				cmd.process(0, "sftp.host=192.85.247.104, sftp.folder=/data/mtccore/source/", null);

				logger.info("After executing sftp command");				
				logger.info("Files in source directory");
				sftpChannel.cd("/data/mtccore/source/");
				v = sftpChannel.ls("*");
				logger.info("Files present in source folder:");
				for (LsEntry entry : v) {
					logger.info(entry.getFilename());
				}	
				logger.info("Files in destination directory:");
				FileStatus[] files = fs.listStatus(new Path("/data/mtccore/test"));
				for (FileStatus fileStatus : files) {
					 String fileName = fileStatus.getPath().toString(); 
					 logger.info(fileName.substring(fileName.lastIndexOf("/") + 1));
				}
				return null;

			}
		});
	}
}
