package etl.cmd;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import etl.engine.ETLCmd;
import etl.util.Util;

public class SftpCmd extends ETLCmd{
	public static final Logger logger = Logger.getLogger(BackupCmd.class);
	
	public static final String cfgkey_incoming_folder = "incoming.folder";
	public static final String cfgkey_sftp_host = "sftp.host";
	public static final String cfgkey_sftp_port = "sftp.port";
	public static final String cfgkey_sftp_user = "sftp.user";
	public static final String cfgkey_sftp_pass = "sftp.pass";
	public static final String cfgkey_sftp_folder = "sftp.folder";
	
	private String incomingFolder;
	private String host;
	private int port;
	private String user;
	private String pass;
	private String fromDir;
	
	public SftpCmd(String wfid, String staticCfg, String inDynCfg, String outDynCfg, String defaultFs){
		super(wfid, staticCfg, inDynCfg, outDynCfg, defaultFs);
		this.incomingFolder = pc.getString(cfgkey_incoming_folder);
		this.host = pc.getString(cfgkey_sftp_host);
		this.port = pc.getInt(cfgkey_sftp_port);
		this.user = pc.getString(cfgkey_sftp_user);
		this.pass = pc.getString(cfgkey_sftp_pass);
		this.fromDir = pc.getString(cfgkey_sftp_folder);
	}
	
	@Override
	public void process(String param) {
		Session session = null;
		ChannelSftp sftpChannel = null;
		try {
			//override param
			logger.info(String.format("param:", param));
			Map<String, String> pm = Util.parseMapParams(param);
			if (pm.containsKey(cfgkey_sftp_host)){
				this.host = pm.get(cfgkey_sftp_host);
			}
			if (pm.containsKey(cfgkey_sftp_port)){
				this.port = Integer.parseInt(pm.get(cfgkey_sftp_port));
			}
			if (pm.containsKey(cfgkey_sftp_user)){
				this.user = pm.get(cfgkey_sftp_user);
			}
			if (pm.containsKey(cfgkey_sftp_pass)){
				this.pass = pm.get(cfgkey_sftp_pass);
			}
			if (pm.containsKey(cfgkey_sftp_folder)){
				this.fromDir = pm.get(cfgkey_sftp_folder);
			}
			//connect
			JSch jsch = new JSch();
            session = jsch.getSession(user, host, port);
            session.setConfig("StrictHostKeyChecking", "no");
            session.setPassword(pass);
            session.connect();
            Channel channel = session.openChannel("sftp");
            channel.connect();
            sftpChannel = (ChannelSftp) channel;
            sftpChannel.cd(fromDir);
            Vector<LsEntry> v = sftpChannel.ls("*");
            for (LsEntry entry:v){
            	String srcFile = fromDir + entry.getFilename();
            	String destFile = incomingFolder + entry.getFilename();
            	logger.info(String.format("get file from %s to %s", srcFile, destFile));
            	//OutputStream fsos = new FileOutputStream(new File(destFile));
            	OutputStream fsos = fs.create(new Path(destFile));
            	InputStream is = sftpChannel.get(srcFile);
            	IOUtils.copy(is, fsos);
            	fsos.close();
            	is.close();
            }
		}catch(Exception e){
			logger.error("", e);
		}finally{
			if (sftpChannel!=null){
				sftpChannel.exit();
			}
			if (session!=null){
				session.disconnect();
			}
		}
	}
}