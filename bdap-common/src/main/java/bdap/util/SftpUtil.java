package bdap.util;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.commons.exec.LogOutputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.ChannelSftp.LsEntry;

public class SftpUtil {
	public static final Logger logger = LogManager.getLogger(SftpUtil.class);
	public static final String PATH_SEPARATOR = "/";
	private static final OutputStream loggerOut = new LogOutputStreamImpl();
	private static final OutputStream loggerErrOut = new LogErrOutputStreamImpl();
	
	private static class LogOutputStreamImpl extends LogOutputStream {
		protected void processLine(String line, int logLevel) {
			logger.debug(line);;
		}
	}
	
	private static class LogErrOutputStreamImpl extends LogOutputStream {
		protected void processLine(String line, int logLevel) {
			logger.error(line);;
		}
	}
	
	public static Session getSession(String host, int port, String user, String pass){
		JSch jsch = new JSch();
		try {
			Session session = jsch.getSession(user, host, port);
			session.setConfig("StrictHostKeyChecking", "no");
			session.setPassword(pass);
			session.connect();
			return session;
		}catch(Exception e){
			logger.error("", e);
			return null;
		}
	}
	
	public static Session getSession(SftpInfo ftpInfo) {
		return getSession(ftpInfo.ip, ftpInfo.port, ftpInfo.user, ftpInfo.passwd);
	}
	
	public static String sendCommand(String command, Session sesConnection) {
		StringBuilder outputBuffer = new StringBuilder();
		Channel channel = null;

		try {
			channel = sesConnection.openChannel("exec");
			((ChannelExec) channel).setCommand(command);
			channel.setInputStream(null);
			channel.setOutputStream(loggerOut);
			((ChannelExec) channel).setErrStream(loggerErrOut);
			channel.connect();

			InputStream commandOutput = channel.getInputStream();
			final byte[] tmp = new byte[1024];
			while (true) {
				while (commandOutput.available() > 0) {
					final int i = commandOutput.read(tmp, 0, 1024);
					if (i < 0) {
						break;
					}
					outputBuffer.append(new String(tmp, 0, i));
				}
				if (channel.isClosed()) {
					logger.debug("exit-status: " + channel.getExitStatus());
					break;
				}
				try {
					Thread.sleep(1000);
				} catch (final Exception e) {
					logger.error(e.getMessage(), e);
				}
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			return null;

		} finally {
			if (channel != null)
				channel.disconnect();
		}

		return outputBuffer.toString();
	}

	//
	public static void sftpMkdir(ChannelSftp sftpChannel, String remoteFile) throws Exception {
		int slash = remoteFile.lastIndexOf(PATH_SEPARATOR);
	    String remotePath = remoteFile.substring(0,slash);
		String[] folders = remotePath.split(PATH_SEPARATOR);
		int i=0;
	    for ( String folder : folders ) {
	    	if ( folder.length() > 0 ) {//ignore empty parts
	    		if (i==0 && remotePath.startsWith(PATH_SEPARATOR)){//for the 1st part of the path, needs to consider absolute or relative path
		    		folder = String.format("/%s", folder);
		    	}
	    		logger.info(String.format("cd:%s", folder));
		        try {
	            	sftpChannel.cd(folder);
	            }
	            catch ( SftpException e ) {
	            	logger.info(String.format("mkdir %s", folder));
	            	sftpChannel.mkdir(folder);
	            	logger.info(String.format("cd:%s", folder));
	            	sftpChannel.cd(folder);
	            }
	            i++;
	        }
	    }
	}

	public static void sftpFromLocal(Session session, String localFile, String remoteFile) throws Exception {
		Channel channel = null;
		try {
			channel = session.openChannel("sftp");
			channel.connect();
			final ChannelSftp sftpChannel = (ChannelSftp) channel;
		    
		    if (Files.isDirectory(new File(localFile).toPath())){
		    	Path localRootPath = Paths.get(localFile);
		    	Files.walk(localRootPath)
		        .filter(Files::isRegularFile)
		        .forEach(lf->{
		        	Path relPath = localRootPath.relativize(lf);
		        	String rfStr = remoteFile + relPath.toString().replace("\\", PATH_SEPARATOR);
		        	try {
		        		logger.info(String.format("try copy local:%s to remote:%s", lf, rfStr));
		        		sftpMkdir(sftpChannel, rfStr);
						sftpChannel.put(lf.toString(), rfStr, ChannelSftp.OVERWRITE);
					} catch (Exception e) {
						logger.error(String.format("try copy local:%s to remote:%s", lf, rfStr), e);
					}
		        });
		    }else{
		    	logger.info(String.format("try copy local:%s to remote:%s", localFile, remoteFile));
		    	sftpMkdir(sftpChannel, remoteFile);
		    	sftpChannel.put(localFile, remoteFile, ChannelSftp.OVERWRITE);
		    }
		} finally {
			if (channel != null)
				channel.disconnect();
		}
	}

	/**
	 * @param host
	 * @param port
	 * @param user
	 * @param pass
	 * @param localFile/localDir, if this is a directory, then all the files will be ftped to its remote counterpart
	 * @param remoteFile/remoteDir
	 */
	public static void sftpFromLocal(SftpInfo ftpInfo, String localFile, String remoteFile) {
		Session session = null;
		try {
			// connect
			session = getSession(ftpInfo.ip, ftpInfo.port, ftpInfo.user, ftpInfo.passwd);
			sftpFromLocal(session, localFile, remoteFile);
			
		} catch (Exception e) {
			logger.error("Exception while processing SFTP:", e);
		} finally {
			if (session != null) {
				session.disconnect();
			}
		}
	}
	
	public static List<String> sftpList(String host, int port, String user, String pass, String remoteDir) {
		Session session = null;
		ChannelSftp sftpChannel = null;
		List<String> fl = new ArrayList<String>();
		try {
			// connect
			JSch jsch = new JSch();
			Channel channel = null;
			session = jsch.getSession(user, host, port);
			session.setConfig("StrictHostKeyChecking", "no");
			session.setPassword(pass);
			session.connect();
			channel = session.openChannel("sftp");
			channel.connect();
			sftpChannel = (ChannelSftp) channel;
			sftpChannel.cd(remoteDir);
			Vector<LsEntry> v = sftpChannel.ls("*");
			for (LsEntry entry : v) {
				fl.add(entry.getFilename());
			}
		} catch (Exception e) {
			logger.error("Exception while processing SFTP:", e);
		} finally {

			if (sftpChannel != null) {
				sftpChannel.exit();
			}
			if (session != null) {
				session.disconnect();
			}
		}
		return fl;
	}
}
