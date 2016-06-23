package etl.cmd.test;

import java.security.PrivilegedExceptionAction;
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
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import etl.cmd.SftpCmd;

public class TestSftpCmdDeleteFileNotEnabled {
	@Test
	public void test1() throws Exception {
		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
	    ugi.doAs(new PrivilegedExceptionAction<Void>() {
	      public Void run() throws Exception {
	        Logger logger = Logger.getLogger(TestSftpCmdConnectionFailure.class);
	    	logger.info("Testing sftpcmd delete not enabled test case");
	    	Configuration conf = new Configuration();
	    	String defaultFS = "hdfs://192.85.247.104:19000";
			conf.set("fs.defaultFS", defaultFS);
			FileSystem fs = FileSystem.get(conf);
			Session session = null;
			ChannelSftp sftpChannel = null;
			String localFolder = "C:\\Users\\rangasap\\workspace\\log.analysis\\preload\\src\\test\\resources\\";
	    	String dfsFolder = "/data/sample/";
	    	String cfg = "sftp_test.properties";
			fs.copyFromLocalFile(new Path(localFolder + cfg), new Path(dfsFolder+cfg));
			
			
			
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
			cmd.process("sftp.host=192.85.247.104, sftp.folder=/data/mtccore/source/, sftp.clean=false", null);
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
}
