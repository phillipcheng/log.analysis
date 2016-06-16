package etl.cmd;

import java.io.IOException;
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
import com.jcraft.jsch.SftpException;

import etl.engine.ETLCmd;
import etl.util.Util;

public class SftpCmd extends ETLCmd{
	public static final Logger logger = Logger.getLogger(SftpCmd.class);
	
	public static final String cfgkey_incoming_folder = "incoming.folder";
	public static final String cfgkey_sftp_host = "sftp.host";
	public static final String cfgkey_sftp_port = "sftp.port";
	public static final String cfgkey_sftp_user = "sftp.user";
	public static final String cfgkey_sftp_pass = "sftp.pass";
	public static final String cfgkey_sftp_folder = "sftp.folder";
	//added for sftp retry
	public static final String cfgkey_sftp_retry = "sftp.getRetryTimes";
	public static final String cfgkey_sftp_connect_retry = "sftp.connectRetryTimes";
	
	private String incomingFolder;
	private String host;
	private int port;
	private String user;
	private String pass;
	private String fromDir;
	private int sftpRetryCount;
	private int sftpConnectRetryCount;
	
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
		int retryCntTemp = 1;
		int sftConnectRetryCntTemp = 1;
		OutputStream fsos = null;
		InputStream is = null;
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
			
			////added for sftp retry
			//conversionException is already handled here.
			sftpRetryCount = pc.getInt(cfgkey_sftp_retry);
			sftpConnectRetryCount = pc.getInt(cfgkey_sftp_connect_retry);
			//connect
			while (retryCntTemp <= sftpRetryCount) {
			try {
					JSch jsch = new JSch();
				    Channel channel = null;
				    session = jsch.getSession(user, host, port);
				    if(session == null){
				    	logger.error("Session is not created.");
				    }
				    session.setConfig("StrictHostKeyChecking", "no");
				    session.setPassword(pass);
				    
				    //retry for connecting to session
				    while(sftConnectRetryCntTemp <= sftpConnectRetryCount){
				    	session.connect();
				    	if(session.isConnected()){
				        	channel = session.openChannel("sftp");
				        }else{
				        	Thread.sleep(60000L);
				        	sftpConnectRetryCount++;
				        	logger.error("Session is not connected.");
				        }
				    }
				    
				    
				    channel.connect();
				    
				    sftpChannel = (ChannelSftp) channel;
				    sftpChannel.cd(fromDir);
				    System.out.println("From Dir:"+fromDir);
				    Vector<LsEntry> v = sftpChannel.ls("*");
				    for (LsEntry entry:v){
				    	String srcFile = fromDir + entry.getFilename();
				    	String destFile = incomingFolder + entry.getFilename();
				    	logger.info(String.format("get file from %s to %s", srcFile, destFile));
				    	//OutputStream fsos = new FileOutputStream(new File(destFile));
				    	fsos = fs.create(new Path(destFile));
				    	is = sftpChannel.get(srcFile);
				    	IOUtils.copy(is, fsos);
				    	try {
							if(fsos != null){
								fsos.close();
							}
							if(is != null){
								is.close();
							}
						} catch (IOException e) {
							// TODO Auto-generated catch block
							logger.error("Error while closing streams.", e);
							e.printStackTrace();
						}
				    	//deleting file one by one
				    	//sftpChannel.rm(srcFile);
				    }
				    //deleting directory
				    //sftpChannel.rmdir(fromDir);
				
			} catch (SftpException e) {
	            if (retryCntTemp == sftpRetryCount) {
	            	logger.info("Problem persists during sftp process:"+ e.getMessage()+" Retried for maximum times. exiting sftp download.");
	            }else{
	            	retryCntTemp++;
	            	logger.info("Problem persists during sftp process:"+ e.getMessage()+" Retrying..." + "Retrying count: "+ retryCntTemp);
		            try {
						Thread.sleep(60000L);
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
				
			}
			}
		}catch(Exception e){
			logger.error("Exception while processing SFTP:", e);
		}finally{
			try {
				if(fsos != null){
					fsos.close();
				}
				if(is != null){
					is.close();
				}
				if (sftpChannel!=null){
					sftpChannel.exit();
				}
				if (session!=null){
					session.disconnect();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				logger.error("Error while executing finally block.", e);
				e.printStackTrace();
			}
		}
	}
}