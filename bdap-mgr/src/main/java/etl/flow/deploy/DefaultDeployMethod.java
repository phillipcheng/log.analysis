package etl.flow.deploy;

import java.io.InputStream;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import bdap.util.HdfsUtil;
import bdap.util.SystemUtil;

public class DefaultDeployMethod implements DeployMethod {
	public static final Logger logger = LogManager.getLogger(DefaultDeployMethod.class);
	private FileSystem fs;
	
	public FileSystem getFs() {
		return fs;
	}

	public DefaultDeployMethod(String remoteUser, String defaultFs) {
		Set<String> ipAddresses = SystemUtil.getMyIpAddresses();
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", defaultFs);
		try {
			if (ipAddresses.contains(defaultFs)){
				fs = FileSystem.get(conf);
				// localDeploy = true;
			}else{
				UserGroupInformation ugi = UserGroupInformation.createProxyUser(remoteUser, UserGroupInformation.getLoginUser());
				ugi.doAs(new PrivilegedExceptionAction<Void>() {
					public Void run() throws Exception {
						fs = FileSystem.get(conf);
						// localDeploy = false;
						return null;
					}
				});
			}
		}catch(Exception e){
			logger.error("", e);
		}
	}
	
	public void createFile(String remotePath, byte[] content) {
		HdfsUtil.writeDfsFile(fs, remotePath, content);
	}

	public void createFile(String remotePath, InputStream inputStream) {
		HdfsUtil.writeDfsFile(fs, remotePath, inputStream);
	}
	
	public void delete(String remotePath, boolean recursive) {
		try {
			fs.delete(new org.apache.hadoop.fs.Path(remotePath), recursive);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	public void copyFromLocalFile(String localPath, String remotePath) {
		try {
			fs.copyFromLocalFile(new Path(localPath), new Path(remotePath));
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}
	
	public void copyFromLocalFile(boolean delSrc, boolean overwrite, String localPath, String remotePath) {
		try {
			fs.copyFromLocalFile(delSrc, overwrite, new Path(localPath), new Path(remotePath));
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}
	
	public List<String> listFiles(String path) {
		return HdfsUtil.listDfsFile(fs, path);
	}

	public List<String> readFile(String path) {
		return HdfsUtil.stringsFromDfsFile(fs, path);
	}

	public boolean exists(String path) {
		try {
			return fs.exists(new Path(path));
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			return false;
		}
	}
}
