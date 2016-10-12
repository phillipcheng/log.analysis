package etl.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.ChannelSftp.LsEntry;

public class SftpUtil {
	public static final Logger logger = LogManager.getLogger(SftpUtil.class);
	
	public static void sftpFromLocal(String host, int port, String user, String pass, String localFile, String remoteFile){
		Session session = null;
		ChannelSftp sftpChannel = null;
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
			int slash = remoteFile.lastIndexOf("/");
		    String remotePath = remoteFile.substring(0,slash);
		    String[] folders = remotePath.split( "/" );
		    for ( String folder : folders ) {
		    	logger.info("FolderName:"+folder);
		        if ( folder.length() > 0 ) {
		            try {
		            	sftpChannel.cd( folder );
		            }
		            catch ( SftpException e ) {
		            	logger.info(String.format("mkdir %s", folder));
		            	sftpChannel.mkdir( folder );
		            	sftpChannel.cd( folder );
		            }
		        }
		    }
		    //sftpChannel.mkdir(remotePath);
			sftpChannel.put(localFile, remoteFile, ChannelSftp.OVERWRITE);
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
