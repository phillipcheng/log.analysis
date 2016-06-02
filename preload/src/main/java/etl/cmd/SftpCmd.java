package etl.cmd;

import java.io.File;
import java.io.FileFilter;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import etl.cmd.dynschema.DynSchemaCmd;
import etl.engine.ETLCmd;
import etl.util.Util;

public class SftpCmd extends ETLCmd{
	public static final Logger logger = Logger.getLogger(BackupCmd.class);
	
	public static final String cfgkey_incoming_folder = "incoming.folder";
	public static final String cfgkey_sftp_host = "sftp.host";
	public static final String cfgkey_sftp_user = "sftp.user";
	public static final String cfgkey_sftp_pass = "sftp.pass";
	public static final String cfgkey_sftp_folder = "sftp.folder";
	
	private String incomingFolder;
	private String[] hosts;
	private String[] users;
	private String[] passes;
	private String[] fromDirs;
	
	public SftpCmd(String wfid, String staticCfg, String inDynCfg, String outDynCfg, String defaultFs){
		super(wfid, staticCfg, inDynCfg, outDynCfg, defaultFs);
		this.incomingFolder = pc.getString(cfgkey_incoming_folder);
		this.hosts = pc.getStringArray(cfgkey_sftp_host);
		this.users = pc.getStringArray(cfgkey_sftp_user);
		this.passes = pc.getStringArray(cfgkey_sftp_pass);
		this.fromDirs = pc.getStringArray(cfgkey_sftp_folder);
	}
	
	@Override
	public void process(String param) {
		try {
			//connect
			
			//get
			
			//remove
		}catch(Exception e){
			logger.error("", e);
		}
	}
}