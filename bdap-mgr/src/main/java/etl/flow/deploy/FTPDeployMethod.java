package etl.flow.deploy;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.IOUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import bdap.util.HdfsUtil;

/* FTP over HDFS */
public class FTPDeployMethod implements DeployMethod {
	public static final Logger logger = LogManager.getLogger(FTPDeployMethod.class);
	public static final String PATH_SEPARATOR = "/";
	private static final String key_server_ip = "ftp.deploy.server.ip";
	private static final String key_server_port = "ftp.deploy.server.port";
	private static final String key_server_user = "ftp.deploy.server.user";
	private static final String key_server_passwd = "ftp.deploy.server.passwd";
	private String serverIp;
	private int port;
	private String userName;
	private String passwd;
	
	public FTPDeployMethod(Configuration pc) {
		serverIp = pc.getString(key_server_ip, "127.0.0.1");
		port = pc.getInt(key_server_port, 21);
		userName = pc.getString(key_server_user, "dbadmin");
		passwd = pc.getString(key_server_passwd, "password");
	}

	public FTPDeployMethod(String serverIp, int port, String userName, String passwd) {
		this.serverIp = serverIp;
		this.port = port;
		this.userName = userName;
		this.passwd = passwd;
	}

	public void createFile(String remotePath, byte[] content) {
		FTPClient f = null;
		InputStream in = null;
		try {
			f = new FTPClient();
			FTPClientConfig config = new FTPClientConfig();
			f.configure(config);
			f.connect(serverIp, port);
		    f.setReportActiveExternalIPAddress("10.0.2.2");
		    f.enterLocalActiveMode();
		    f.login(userName, passwd);
		    
			/* Try to get the root path from it if it's a URL */
			remotePath = HdfsUtil.getRootPath(remotePath);
			
		    f.deleteFile(remotePath);
		    in = new ByteArrayInputStream(content);
		    f.storeFile(remotePath, in);
		    
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
			
		} finally {
			if (in != null)
				try {
					in.close();
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
				}
			if (f != null)
				try {
					f.disconnect();
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
				}
		}
	}
	
	private void removeDirectory(FTPClient ftpClient, String parentDir, String currentDir, boolean recursive) throws IOException {
        String dirToList = parentDir;
        if (!currentDir.equals("")) {
            dirToList += PATH_SEPARATOR + currentDir;
        }
        
        FTPFile[] subFiles = ftpClient.listFiles(dirToList);
 
        if (subFiles != null && subFiles.length > 0) {
            for (FTPFile aFile : subFiles) {
                String currentFileName = aFile.getName();
                if (currentFileName.equals(".") || currentFileName.equals("..")) {
                    // skip parent directory and the directory itself
                    continue;
                }
                String filePath = parentDir + PATH_SEPARATOR + currentDir + PATH_SEPARATOR
                        + currentFileName;
                if (currentDir.equals("")) {
                    filePath = parentDir + PATH_SEPARATOR + currentFileName;
                }
 
                if (aFile.isDirectory() && recursive) {
                    // remove the sub directory
                    removeDirectory(ftpClient, dirToList, currentFileName, recursive);
                } else {
                    // delete the file
                    boolean deleted = ftpClient.deleteFile(filePath);
                    if (deleted) {
                        System.out.println("DELETED the file: " + filePath);
                    } else {
                        System.out.println("CANNOT delete the file: "
                                + filePath);
                    }
                }
            }
 
            // finally, remove the directory itself
            boolean removed = ftpClient.removeDirectory(dirToList);
            if (removed) {
                System.out.println("REMOVED the directory: " + dirToList);
            } else {
                System.out.println("CANNOT remove the directory: " + dirToList);
            }
        }
	}
	
	public void delete(String remotePath, boolean recursive) {
		FTPClient f = null;
		try {
			f = new FTPClient();
			FTPClientConfig config = new FTPClientConfig();
			f.configure(config);
			f.connect(serverIp, port);
		    f.setReportActiveExternalIPAddress("10.0.2.2");
		    f.enterLocalActiveMode();
		    f.login(userName, passwd);
	        
	        String fileSt = f.getStatus("/");
	        if (fileSt != null && fileSt.length() > 0 && fileSt.startsWith("d"))
	        	removeDirectory(f, remotePath, "", recursive);
	        else
	        	f.deleteFile(remotePath);
		    
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		} finally {
			if (f != null)
				try {
					f.disconnect();
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
				}
		}
	}
	
	private static void mkdirs(FTPClient ftpClient, String remoteDir) throws IOException {
		int i;
		int n;
		String dir;
		boolean dirExists = true;
		// tokenize the string and attempt to change into each directory level.
		// If you cannot, then start creating.
		String[] directories = remoteDir.split(PATH_SEPARATOR);
		if (remoteDir.endsWith(PATH_SEPARATOR))
			n = directories.length;
		else
			n = directories.length - 1;
		for (i = 0; i < n; i ++) {
			dir = directories[i];
			if (!dir.isEmpty()) {
				if (dirExists) {
					dirExists = ftpClient.changeWorkingDirectory(dir);
				}
				if (!dirExists) {
					if (!ftpClient.makeDirectory(dir)) {
						throw new IOException("Unable to create remote directory '" + dir + "'.  error='"
								+ ftpClient.getReplyString() + "'");
					}
					if (!ftpClient.changeWorkingDirectory(dir)) {
						throw new IOException("Unable to change into newly created remote directory '" + dir
								+ "'.  error='" + ftpClient.getReplyString() + "'");
					}
				}
			}
		}
	}
	
	public void copyFromLocalFile(String localPath, String remotePath) {
		final FTPClient f = new FTPClient();
		try {
			FTPClientConfig config = new FTPClientConfig();
			f.configure(config);
			f.connect(serverIp, port);
		    f.setReportActiveExternalIPAddress("10.0.2.2");
		    f.enterLocalActiveMode();
		    f.login(userName, passwd);
		    
		    File localFile = new File(localPath);

		    if (Files.isDirectory(localFile.toPath())){
		    	Path localRootPath = Paths.get(localPath);
		    	Files.walk(localRootPath)
		        .filter(Files::isRegularFile)
		        .forEach(lf->{
		        	Path relPath = localRootPath.relativize(lf);
		        	String rfStr;
		        	if ("\\".equals(File.separator))
		        		rfStr = remotePath + relPath.toString().replace(File.separator, PATH_SEPARATOR);
		        	else
		        		rfStr = remotePath + relPath.toString();

		    		InputStream in = null;
	        		logger.info(String.format("try copy local:%s to remote:%s", lf, rfStr));
	        		
		        	try {
		        		mkdirs(f, rfStr);
			        	in = Files.newInputStream(lf);
			        	f.storeFile(rfStr, in);
					} catch (Exception e) {
						logger.error(String.format("try copy local:%s to remote:%s", lf, rfStr), e);
					} finally {
	        			if (in != null)
							try {
								in.close();
							} catch (IOException e) {
								logger.error(e.getMessage(), e);
							}
	        		}
		        });
		    }else{
	    		InputStream in = null;
		    	logger.info(String.format("try copy local:%s to remote:%s", localPath, remotePath));
		    	mkdirs(f, remotePath);
        		try {
	        		f.deleteFile(remotePath);
	        		in = Files.newInputStream(Paths.get(localPath));
	        		f.storeFile(remotePath, in);
        		} finally {
        			if (in != null)
        				in.close();
        		}
		    }
		    
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		} finally {
			if (f != null)
				try {
					f.disconnect();
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
				}
		}
	}
	
	/* TODO implement */
	public void copyFromLocalFile(boolean delSrc, boolean overwrite, String localPath, String remotePath) {
		this.copyFromLocalFile(localPath, remotePath);
	}
	
	public List<String> listFiles(String path) {
		List<String> result = new ArrayList<String>();
		FTPClient f = null;
		try {
			f = new FTPClient();
			FTPClientConfig config = new FTPClientConfig();
			f.configure(config);
			f.connect(serverIp, port);
		    f.setReportActiveExternalIPAddress("10.0.2.2");
		    f.enterLocalActiveMode();
		    f.login(userName, passwd);
		    FTPFile[] files = f.listFiles(path);
		    if (files != null) {
		    	for (FTPFile file: files)
		    		result.add(file.getName());
		    	logger.info(files);
		    }
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		} finally {
			if (f != null)
				try {
					f.disconnect();
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
				}
		}
		return result;
	}
	
	public List<String> readFile(String path) {
		FTPClient f = null;
		try {
			f = new FTPClient();
			FTPClientConfig config = new FTPClientConfig();
			f.configure(config);
			f.connect(serverIp, port);
		    f.setReportActiveExternalIPAddress("10.0.2.2");
		    f.enterLocalActiveMode();
		    f.login(userName, passwd);
		    return IOUtils.readLines(f.retrieveFileStream(path), StandardCharsets.UTF_8);
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		} finally {
			if (f != null)
				try {
					f.disconnect();
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
				}
		}
		return Collections.emptyList();
	}

}
