package bdap.util;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

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
	public static String sendCommand(String command, Session sesConnection) {
		StringBuilder outputBuffer = new StringBuilder();
		try {
			Channel channel = sesConnection.openChannel("exec");
			((ChannelExec)channel).setCommand(command);
			InputStream commandOutput = channel.getInputStream();
			channel.connect();
			int readByte = commandOutput.read();
			while(readByte != 0xffffffff) {
				outputBuffer.append((char)readByte);
				readByte = commandOutput.read();
			}
			channel.disconnect();
		}catch(Exception e){
			logger.error("", e);
			return null;
		}
		return outputBuffer.toString();
	}

	//
	private static void sftpMkdir(ChannelSftp sftpChannel, String remoteFile) throws Exception{
		int slash = remoteFile.lastIndexOf("/");
	    String remotePath = remoteFile.substring(0,slash);
		String[] folders = remotePath.split("/");
		int i=0;
	    for ( String folder : folders ) {
	    	if ( folder.length() > 0 ) {//ignore empty parts
	    		if (i==0 && remotePath.startsWith("/")){//for the 1st part of the path, needs to consider absolute or relative path
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

	/**
	 * @param host
	 * @param port
	 * @param user
	 * @param pass
	 * @param localFile/localDir, if this is a directory, then all the files will be ftped to its remote counterpart
	 * @param remoteFile/remoteDir
	 */
	public static void sftpFromLocal(SftpInfo ftpInfo, String localFile, String remoteFile){
		Session session = null;
		try {
			// connect
			session = getSession(ftpInfo.ip, ftpInfo.port, ftpInfo.user, ftpInfo.passwd);
			Channel channel = null;
			channel = session.openChannel("sftp");
			channel.connect();
			final ChannelSftp sftpChannel = (ChannelSftp) channel;
		    
		    if (Files.isDirectory(new File(localFile).toPath())){
		    	Path localRootPath = Paths.get(localFile);
		    	Files.walk(localRootPath)
		        .filter(Files::isRegularFile)
		        .forEach(lf->{
		        	Path relPath = localRootPath.relativize(lf);
		        	String rfStr = remoteFile + relPath.toString().replace("\\", "/");
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
		} catch (Exception e) {
			logger.error("Exception while processing SFTP:", e);
		} finally {
			if (session != null) {
				session.disconnect();
			}
		}
	}
	
	public static List<String> sftpList(String host, int port, String user, String pass, String remoteDir){
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
