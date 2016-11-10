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
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.receiver.Receiver;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;

import bdap.util.HdfsUtil;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import etl.engine.ETLCmd;
import etl.spark.SparkReciever;
import etl.util.ParamUtil;
import etl.util.ScriptEngineUtil;
import etl.util.VarType;
import scala.Tuple2;

public class SftpCmd extends ETLCmd implements SparkReciever{
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(SftpCmd.class);

	//cfgkey
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
	public static final String cfgkey_file_limit ="file.limit";
	public static final String cfgkey_names_only="sftp.names.only";
	public static final String paramkey_src_file="src.file";

	public static final String separator = "/";
	

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
	private boolean sftpClean=false;
	private int fileLimit=0;
	private boolean sftpNamesOnly = false; //when this is true, we lost the capability to enable the overlap of the multiple SftpCmd to process the same dir.

	public SftpCmd(){
		super();
	}
	
	public SftpCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
		String incomingFolderExp = super.getCfgString(cfgkey_incoming_folder, null);
		if (incomingFolderExp!=null){
			this.incomingFolder = (String) ScriptEngineUtil.eval(incomingFolderExp, VarType.STRING, super.getSystemVariables());
		}
		this.host = super.getCfgString(cfgkey_sftp_host, null);
		this.port = super.getCfgInt(cfgkey_sftp_port, 22);
		this.user = super.getCfgString(cfgkey_sftp_user, null);
		this.pass = super.getCfgString(cfgkey_sftp_pass, null);
		this.fromDirs = super.getCfgStringArray(cfgkey_sftp_folder);
		String fileFilterExp = super.getCfgString(cfgkey_file_filter, null);
		if (fileFilterExp!=null){
			this.fileFilter = (String) ScriptEngineUtil.eval(fileFilterExp, VarType.STRING, super.getSystemVariables());
		}
		this.sftpGetRetryCount = super.getCfgInt(cfgkey_sftp_get_retry, 3);
		this.sftpGetRetryWait =  super.getCfgInt(cfgkey_sftp_get_retry_wait, 10000);//
		this.sftpConnectRetryCount = super.getCfgInt(cfgkey_sftp_connect_retry, 3);
		this.sftpConnectRetryWait =  super.getCfgInt(cfgkey_sftp_connect_retry_wait, 15000);//
		this.sftpClean = super.getCfgBoolean(cfgkey_sftp_clean, false);
		this.fileLimit = super.getCfgInt(cfgkey_file_limit, 0);
		this.sftpNamesOnly = super.getCfgBoolean(cfgkey_names_only, false);
		if (this.sftpNamesOnly){
			this.sftpClean=false;//when only return name, then the file can't be cleaned since someone is going to get this later and remove it by himself,
		}
	}

	public List<String> process(long mapKey, String row, Receiver<String> r){
		logger.info(String.format("param: %s", row));
		Map<String, String> pm = ParamUtil.parseMapParams(row);
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
		
		Session session = null;
		ChannelSftp sftpChannel = null;
		int getRetryCntTemp = 1;
		int sftConnectRetryCntTemp = 1;
		OutputStream fsos = null;
		InputStream is = null;
		List<String> files = new ArrayList<String>();
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
					session.setTimeout(this.sftpConnectRetryWait);
					session.connect();
					channel = session.openChannel("sftp");
					break;
				} catch (Exception e) {
					if (sftConnectRetryCntTemp == sftpConnectRetryCount) {
						logger.error("Reached maximum number of times for connecting session.");
						throw new SftpException(0, 
								String.format("Reached maximum number of times for connecting sftp session to %s", this.host));
					}
					logger.error("Session connection failed. retrying..." + sftConnectRetryCntTemp);
					sftConnectRetryCntTemp++;
				}
			}

			int numProcessed=0;
			channel.connect();
			sftpChannel = (ChannelSftp) channel;
			boolean workflag=true;
			for (String fromDir:fromDirs){
				if (!fromDir.endsWith(separator)){
					fromDir += separator;
				}
				logger.info(String.format("sftp folder:%s, filter:%s", fromDir, fileFilter));
				sftpChannel.cd(fromDir);
				Vector<LsEntry> v = sftpChannel.ls(fileFilter);
				if (workflag){
					for (LsEntry entry : v) {
						if (!entry.getAttrs().isDir()){
							String srcFile = fromDir + entry.getFilename();
							String fileNorm = entry.getFilename().replace(",", "_");
							String destFile = incomingFolder + fileNorm;
							if (this.sftpNamesOnly){
								logger.info(String.format("find file %s", srcFile));
								files.add(srcFile);
							}else{
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
								files.add(destFile);
								if (r!=null){//give to receiver
									r.store(destFile);
								}
							}
							numProcessed++;
							if (this.fileLimit>0 && numProcessed>=this.fileLimit){
								logger.info(String.format("file limit %d reached, stop processing.", numProcessed));
								workflag=false;
								break;
							}
						}
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
		if (this.sftpNamesOnly){//write out the output file containing sftpserver, sftp user name and file name
			List<String> outputList = new ArrayList<String>();
			String[] argNames = new String[]{cfgkey_sftp_host, cfgkey_sftp_user, paramkey_src_file};
			for (String srcFile: files){
				String[] argValues = new String[]{this.host, this.user, srcFile};
				outputList.add(ParamUtil.makeMapParams(argNames, argValues));
			}
			HdfsUtil.writeDfsFile(getFs(), String.format("%s%s", this.getIncomingFolder(), String.valueOf(mapKey)), outputList);
			return outputList;
		}else{
			return files;
		}
	}
	
	@Override
	public void sparkRecieve(Receiver<String> r) {
		process(0, null, r);
	}
	
	@Override
	public List<String> sgProcess(){
		List<String> ret = process(0, null, null);
		int fileNumberTransfer=ret.size();
		List<String> logInfo = new ArrayList<String>();
		logInfo.add(fileNumberTransfer + "");
		return logInfo;
	}
	
	@Override
	public Map<String, Object> mapProcess(long offset, String row, Mapper<LongWritable, Text, Text, Text>.Context context){
		//override param
		Map<String, Object> retMap = new HashMap<String, Object>();
		List<String> outputList = process(offset, row, null);
		List<String> logInfo = new ArrayList<String>();
		logInfo.add(String.valueOf(outputList.size()));
		retMap.put(ETLCmd.RESULT_KEY_LOG, logInfo);
		return retMap;
	}
	
	@Override
	public JavaRDD<String> sparkProcess(JavaRDD<String> input, JavaSparkContext jsc){
		List<String> lines = input.collect();
		List<String> ret = new ArrayList<String>();
		for (int i=0; i<lines.size(); i++){
			String row = lines.get(i);
			List<String> fileList = process(i, row, null);
			ret.addAll(fileList);
		}
		return jsc.parallelize(ret);
	}
	
	@Override
	public boolean hasReduce(){
		return false;
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

	public int getSftpGetRetryCount() {
		return sftpGetRetryCount;
	}

	public void setSftpGetRetryCount(int sftpGetRetryCount) {
		this.sftpGetRetryCount = sftpGetRetryCount;
	}

	public int getSftpGetRetryWait() {
		return sftpGetRetryWait;
	}

	public void setSftpGetRetryWait(int sftpGetRetryWait) {
		this.sftpGetRetryWait = sftpGetRetryWait;
	}

	public int getSftpConnectRetryCount() {
		return sftpConnectRetryCount;
	}

	public void setSftpConnectRetryCount(int sftpConnectRetryCount) {
		this.sftpConnectRetryCount = sftpConnectRetryCount;
	}

	public int getSftpConnectRetryWait() {
		return sftpConnectRetryWait;
	}

	public void setSftpConnectRetryWait(int sftpConnectRetryWait) {
		this.sftpConnectRetryWait = sftpConnectRetryWait;
	}

	public boolean isSftpClean() {
		return sftpClean;
	}

	public void setSftpClean(boolean sftpClean) {
		this.sftpClean = sftpClean;
	}

	public int getFileLimit() {
		return fileLimit;
	}

	public void setFileLimit(int fileLimit) {
		this.fileLimit = fileLimit;
	}
}