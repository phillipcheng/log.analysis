package etl.cmd;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import etl.engine.ETLCmd;
import etl.util.ScriptEngineUtil;
import etl.util.Util;
import etl.util.VarType;
import scala.Tuple2;

public class SftpCmd extends ETLCmd {
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(SftpCmd.class);

	public static final String cfgkey_incoming_folder = "incoming.folder";
	public static final String cfgkey_sftp_host = "sftp.host";
	public static final String cfgkey_sftp_port = "sftp.port";
	public static final String cfgkey_sftp_user = "sftp.user";
	public static final String cfgkey_sftp_pass = "sftp.pass";
	public static final String cfgkey_sftp_folder = "sftp.folder";
	public static final String cfgkey_file_filter="file.filter";
	public static final String cfgkey_sftp_get_retry = "sftp.getRetryTimes";
	public static final String cfgkey_sftp_get_retry_wait = "sftp.getRetryWait";
	public static final String cfgkey_sftp_connect_retry = "sftp.connectRetryTimes";
	public static final String cfgkey_sftp_connect_retry_wait = "sftp.connectRetryWait";
	public static final String cfgkey_sftp_clean = "sftp.clean";

	private String incomingFolder;
	private String host;
	private int port;
	private String user;
	private String pass;
	private String[] fromDirs;
	private String fileFilter = "*";
	private int sftpGetRetryCount;
	private int sftpGetRetryWait;
	private int sftpConnectRetryCount;
	private int sftpConnectRetryWait;
	private boolean sftpClean;

	public SftpCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		super(wfName, wfid, staticCfg, defaultFs, otherArgs);
		String incomingFolderExp = pc.getString(cfgkey_incoming_folder, null);
		if (incomingFolderExp!=null){
			this.incomingFolder = (String) ScriptEngineUtil.eval(incomingFolderExp, VarType.STRING, super.getSystemVariables());
		}
		this.host = pc.getString(cfgkey_sftp_host);
		this.port = pc.getInt(cfgkey_sftp_port);
		this.user = pc.getString(cfgkey_sftp_user);
		this.pass = pc.getString(cfgkey_sftp_pass);
		this.fromDirs = pc.getStringArray(cfgkey_sftp_folder);
		String fileFilterExp = pc.getString(cfgkey_file_filter, null);
		if (fileFilterExp!=null){
			this.fileFilter = (String) ScriptEngineUtil.eval(fileFilterExp, VarType.STRING, super.getSystemVariables());
		}
		this.sftpGetRetryCount = pc.getInt(cfgkey_sftp_get_retry);
		this.sftpGetRetryWait =  pc.getInt(cfgkey_sftp_get_retry_wait, 10000);//
		this.sftpConnectRetryCount = pc.getInt(cfgkey_sftp_connect_retry);
		this.sftpConnectRetryWait =  pc.getInt(cfgkey_sftp_connect_retry_wait, 15000);//
		this.sftpClean = pc.getBoolean(cfgkey_sftp_clean);
	}

	public List<Tuple2<String, String>> iProcess(){
		Session session = null;
		ChannelSftp sftpChannel = null;
		int getRetryCntTemp = 1;
		int sftConnectRetryCntTemp = 1;
		OutputStream fsos = null;
		InputStream is = null;
		List<Tuple2<String, String>> files = new ArrayList<Tuple2<String, String>>();
		try {
			// connect
			JSch jsch = new JSch();
			Channel channel = null;
			session = jsch.getSession(user, host, port);
			session.setConfig("StrictHostKeyChecking", "no");
			session.setPassword(pass);

			// retry for session connect
			while (sftConnectRetryCntTemp <= sftpConnectRetryCount) {
				try {
					session.connect();
					channel = session.openChannel("sftp");
					break;
				} catch (Exception e) {
					if (sftConnectRetryCntTemp == sftpConnectRetryCount) {
						logger.error("Reached maximum number of times for connecting session.");
						throw new SftpException(0, "Session not connected");
					}
					logger.error("Session is not connected. retrying..." + sftConnectRetryCntTemp);
					Thread.sleep(this.sftpConnectRetryWait);
					sftConnectRetryCntTemp++;
				}
			}

			channel.connect();
			sftpChannel = (ChannelSftp) channel;
			for (String fromDir:fromDirs){
				logger.info(String.format("sftp folder:%s, filter:%s", fromDir, fileFilter));
				sftpChannel.cd(fromDir);
				Vector<LsEntry> v = sftpChannel.ls(fileFilter);
				for (LsEntry entry : v) {
					if (!entry.getAttrs().isDir()){
						String srcFile = fromDir + entry.getFilename();
						String fileNorm = entry.getFilename().replace(",", "_");
						String destFile = incomingFolder + fileNorm;
						logger.info(String.format("put file to %s from %s", destFile, srcFile));
						getRetryCntTemp = 1;// reset the count to 1 for every file
						while (getRetryCntTemp <= sftpGetRetryCount) {
							try {
								fsos = fs.create(new Path(destFile));
								is = sftpChannel.get(srcFile);
								IOUtils.copy(is, fsos);
								break;
							} catch (Exception e) {
								logger.error("Exception during transferring the file.", e);
								if (getRetryCntTemp == sftpGetRetryCount) {
									logger.info("Copying" + srcFile + "failed Retried for maximum times");
									break;
								}
								getRetryCntTemp++;
								logger.info("Copying" + srcFile + "failed,Retrying..." + getRetryCntTemp);
								Thread.sleep(this.sftpGetRetryWait);
							} finally {
								if (fsos != null) {
									fsos.close();
								}
								if (is != null) {
									is.close();
								}
							}
						}
						// deleting file one by one if sftp.clean is enabled
						if (sftpClean) {
							logger.info("Deleting file:" + srcFile);
							sftpChannel.rm(srcFile);
						}
						files.add(new Tuple2<String, String>(wfid, destFile));
					}
				}
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
		return files;
	}
	
	@Override
	public JavaRDD<Tuple2<String, String>> sparkProcess(JavaRDD<Tuple2<String, String>> input, JavaSparkContext jsc){
		if (input!=null){
			//map over input
			return null;
		}else{
			List<Tuple2<String, String>> ret = iProcess();
			return jsc.parallelize(ret);
		}
	}
	
	@Override
	public List<String> sgProcess(){
		List<Tuple2<String, String>> ret = iProcess();
		int fileNumberTransfer=ret.size();
		List<String> logInfo = new ArrayList<String>();
		logInfo.add(fileNumberTransfer + "");
		return logInfo;
	}
	
	@Override
	public Map<String, Object> mapProcess(long offset, String row, Mapper<LongWritable, Text, Text, Text>.Context context){
		//override param
		logger.info(String.format("param: %s", row));
		Map<String, String> pm = Util.parseMapParams(row);
		if (pm.containsKey(cfgkey_sftp_host)){
			this.host = pm.get(cfgkey_sftp_host);
		}
		if (pm.containsKey(cfgkey_sftp_port)) {
			this.port = Integer.parseInt(pm.get(cfgkey_sftp_port));
		}
		if (pm.containsKey(cfgkey_sftp_user)) {
			this.user = pm.get(cfgkey_sftp_user);
		}
		if (pm.containsKey(cfgkey_sftp_pass)) {
			this.pass = pm.get(cfgkey_sftp_pass);
		}
		if (pm.containsKey(cfgkey_sftp_folder)) {
			this.fromDirs = new String[]{pm.get(cfgkey_sftp_folder)};
		}
		if (pm.containsKey(cfgkey_file_filter)) {
			String fileFilterExp = pm.get(cfgkey_file_filter);
			if (fileFilterExp!=null){
				this.fileFilter = (String) ScriptEngineUtil.eval(fileFilterExp, VarType.STRING, super.getSystemVariables());
			}
		}
		if (pm.containsKey(cfgkey_sftp_get_retry)) {
			this.sftpGetRetryCount = Integer.parseInt(pm.get(cfgkey_sftp_get_retry));
		}
		if (pm.containsKey(cfgkey_sftp_connect_retry)) {
			this.sftpConnectRetryCount = Integer.parseInt(pm.get(cfgkey_sftp_connect_retry));
		}
		if (pm.containsKey(cfgkey_sftp_clean)) {
			sftpClean = new Boolean(pm.get(cfgkey_sftp_clean));
		}
		if (pm.containsKey(cfgkey_incoming_folder)) {
			String incomingFolderExp = pm.get(cfgkey_incoming_folder);
			if (incomingFolderExp!=null){
				this.incomingFolder = (String) ScriptEngineUtil.eval(incomingFolderExp, VarType.STRING, super.getSystemVariables());
			}
		}
		
		Map<String, Object> retMap = new HashMap<String, Object>();
		List<String> logInfo = sgProcess();
		retMap.put(ETLCmd.RESULT_KEY_LOG, logInfo);
		
		return retMap;
	}

	public String[] getFromDirs() {
		return fromDirs;
	}

	public void setFromDirs(String[] fromDirs) {
		this.fromDirs = fromDirs;
	}

	public String getIncomingFolder() {
		return incomingFolder;
	}

	public void setIncomingFolder(String incomingFolder) {
		this.incomingFolder = incomingFolder;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPass() {
		return pass;
	}

	public void setPass(String pass) {
		this.pass = pass;
	}
}