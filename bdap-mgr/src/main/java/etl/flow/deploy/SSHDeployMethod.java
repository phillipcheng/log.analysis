package etl.flow.deploy;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.Session;

import bdap.util.HdfsUtil;
import bdap.util.SftpInfo;
import bdap.util.SftpUtil;

/* Via sftp files to local & ssh to run bin/hdfs dfs -copyFromLocal */
public class SSHDeployMethod extends SftpInfo implements DeployMethod {
	public static final Logger logger = LogManager.getLogger(SSHDeployMethod.class);
	private static final String key_hadoop_home = "hadoop.home";
	private static final String key_tmp_dir = "ssh.deploy.tmp.dir";
	private static final String key_server_ip = "ssh.deploy.server.ip";
	private static final String key_server_port = "ssh.deploy.server.port";
	private static final String key_server_user = "ssh.deploy.server.user";
	private static final String key_server_passwd = "ssh.deploy.server.passwd";
	private String tmpDir;
	private String hadoopHome;
	private Session session;
	
	protected void finalize() throws Throwable {
		super.finalize();
		
		if (session != null) {
			session.disconnect();
			session = null;
		}
	}

	public SSHDeployMethod(String ip, int port, String user, String passwd, String hadoopHome, String tmpDir) {
		super(user, passwd, ip, port);
		
		if (!tmpDir.endsWith(SftpUtil.PATH_SEPARATOR))
			tmpDir = tmpDir + SftpUtil.PATH_SEPARATOR;
		
		if (!hadoopHome.endsWith(SftpUtil.PATH_SEPARATOR))
			hadoopHome = hadoopHome + SftpUtil.PATH_SEPARATOR;
		
		this.tmpDir = tmpDir;
		this.hadoopHome = hadoopHome;
	}

	public SSHDeployMethod(Configuration pc) {
		this(pc.getString(key_server_ip, "127.0.0.1"), 
				pc.getInt(key_server_port, 22),
				pc.getString(key_server_user, "dbadmin"),
				pc.getString(key_server_passwd, "password"),
				pc.getString(key_hadoop_home, "/opt/hadoop-2.7.3"),
				pc.getString(key_tmp_dir, "/tmp"));
	}

	public void createFile(String remotePath, byte[] content) {
		/* Try to get the root path from it if it's a URL */
		remotePath = HdfsUtil.getRootPath(remotePath);
		
		String tmpRemotePath = tmpDir + (remotePath != null && remotePath.startsWith(Path.SEPARATOR) ? remotePath.substring(1) : "");
		ChannelSftp channel = null;

		if (session == null || !session.isConnected())
			session = SftpUtil.getSession(this);

		try {
			channel = (ChannelSftp) session.openChannel("sftp");
			channel.connect();
	
			SftpUtil.sftpMkdir(channel, tmpRemotePath);
			
			InputStream in = null;
	
			try {
				in = new ByteArrayInputStream(content);
				channel.put(in, tmpRemotePath, ChannelSftp.OVERWRITE);
				logger.info("put the file: {}", remotePath);
				
			} finally {
				if (in != null)
					in.close();
			}

			String output = SftpUtil.sendCommand(hadoopHome + "bin/hdfs dfs -copyFromLocal -f " + tmpRemotePath + " " + remotePath, session);
			logger.debug(output);
			
			output = SftpUtil.sendCommand("rm -f " + tmpRemotePath, session);
			logger.debug(output);
			
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			if (channel != null)
				channel.disconnect();
		}
	}

	public void delete(String remotePath, boolean recursive) {
		try {
			String output;
			
			if (session == null || !session.isConnected())
				session = SftpUtil.getSession(this);
			
			if (recursive)
				output = SftpUtil.sendCommand(hadoopHome + "bin/hdfs dfs -rm -r -f " + remotePath, session);
			else
				output = SftpUtil.sendCommand(hadoopHome + "bin/hdfs dfs -rm -f " + remotePath, session);
				
			logger.debug(output);
			
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}
	
	public void copyFromLocalFile(String localPath, String remotePath) {
		String tmpRemotePath = tmpDir + (remotePath != null && remotePath.startsWith(Path.SEPARATOR) ? remotePath.substring(1) : "");
		try {
			if (session == null || !session.isConnected())
				session = SftpUtil.getSession(this);
			
			SftpUtil.sftpFromLocal(session, localPath, tmpRemotePath);
			
			String output = SftpUtil.sendCommand(hadoopHome + "bin/hdfs dfs -copyFromLocal -f " + tmpRemotePath + " " + remotePath, session);
			logger.debug(output);
			
			output = SftpUtil.sendCommand("rm -f " + tmpRemotePath, session);
			logger.debug(output);
			
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}
	
	/* TODO implement */
	public void copyFromLocalFile(boolean delSrc, boolean overwrite, String localPath, String remotePath) {
		this.copyFromLocalFile(localPath, remotePath);
	}
	
	public List<String> listFiles(String path) {
		List<String> result = Collections.emptyList();
		try {
			String output;
			int i;
			
			if (session == null || !session.isConnected())
				session = SftpUtil.getSession(this);
			
			output = SftpUtil.sendCommand(hadoopHome + "bin/hdfs dfs -ls " + path, session);

		    String[] lines = output.split("\\s*\\r?\\n\\s*");
		    String t;
		    if (lines != null && lines.length > 1) {
		    	result = new ArrayList<String>(Arrays.asList(lines));
		    	result.remove(0);
		    	for (i = 0; i < result.size(); i ++) {
		    		t = result.get(i);
		    		result.set(i, t.substring(t.indexOf(Path.SEPARATOR_CHAR)));
		    	}
		    }
		    
		    logger.debug("Number of files: {}", result.size());
			
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		
		return result;
	}
	
	public List<String> readFile(String path) {
		List<String> result = Collections.emptyList();
		try {
			String output;
			
			if (session == null || !session.isConnected())
				session = SftpUtil.getSession(this);
			
			output = SftpUtil.sendCommand(hadoopHome + "bin/hdfs dfs -text " + path, session);

		    String[] lines = output.split("\\s*\\r?\\n\\s*");
	    	result = new ArrayList<String>(Arrays.asList(lines));
			
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		
		return result;
	}

	public void close() {
		if (session != null) {
			session.disconnect();
			session = null;
		}
	}
}
