package etl.cmd;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;

import bdap.util.HdfsUtil;
import bdap.util.ParamUtil;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import etl.engine.ETLCmd;
import etl.engine.ProcessMode;
import etl.util.ConfigKey;
import etl.util.ScriptEngineUtil;
import etl.util.VarType;
import scala.Tuple2;

public class SftpCmd extends ETLCmd {
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(SftpCmd.class);

	//cfgkey
	public static final @ConfigKey String cfgkey_incoming_folder = "incoming.folder";
	public static final @ConfigKey String cfgkey_sftp_host = "sftp.host";
	public static final @ConfigKey(type=Integer.class,defaultValue="22") String cfgkey_sftp_port = "sftp.port";
	public static final @ConfigKey String cfgkey_sftp_user = "sftp.user";
	public static final @ConfigKey String cfgkey_sftp_pass = "sftp.pass";
	public static final @ConfigKey(type=String[].class) String cfgkey_sftp_folder = "sftp.folder";
	public static final @ConfigKey(type=Boolean.class) String cfgkey_sftp_folder_recursive = "sftp.folder.recursive";
	public static final @ConfigKey String cfgkey_file_filter="file.filter";
	public static final @ConfigKey(type=Integer.class,defaultValue="3") String cfgkey_sftp_get_retry = "sftp.getRetryTimes";
	public static final @ConfigKey(type=Integer.class,defaultValue="10000") String cfgkey_sftp_get_retry_wait = "sftp.getRetryWait";
	public static final @ConfigKey(type=Integer.class,defaultValue="3") String cfgkey_sftp_connect_retry = "sftp.connectRetryTimes";
	public static final @ConfigKey(type=Integer.class,defaultValue="15000") String cfgkey_sftp_connect_retry_wait = "sftp.connectRetryWait";
	public static final @ConfigKey(type=Boolean.class) String cfgkey_sftp_clean = "sftp.clean";
	public static final @ConfigKey(type=Boolean.class) String cfgkey_delete_only="delete.only";//just for test
	public static final @ConfigKey(type=Integer.class) String cfgkey_file_limit ="file.limit";
	public static final @ConfigKey(type=Boolean.class) String cfgkey_names_only="sftp.names.only";
	public static final @ConfigKey(defaultValue="default") String cfgkey_output_key="output.key";
	public static final String paramkey_src_file="src.file";

	public static final String separator = "/";
	private static final String output_default_key="default";

	private String incomingFolder;
	private String host;
	private int port;
	private String user;
	private String pass;
	private String[] fromDirs;
	private String fileFilter = "*";
	private boolean recursive;
	private int sftpGetRetryCount;
	private int sftpGetRetryWait;
	private int sftpConnectRetryCount;
	private int sftpConnectRetryWait;
	private boolean sftpClean=false;
	private int fileLimit=0;
	private boolean sftpNamesOnly = false; //when this is true, we lost the capability to enable the overlap of the multiple SftpCmd to process the same dir.
	private boolean deleteOnly=false;
	private String outputKey;
	
	public SftpCmd(){
		super();
	}
	
	public SftpCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	public SftpCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs, ProcessMode pm){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, pm);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs, ProcessMode pm){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
		String incomingFolderExp = super.getCfgString(cfgkey_incoming_folder, null);
		if (incomingFolderExp!=null){
			this.incomingFolder = (String) ScriptEngineUtil.eval(incomingFolderExp, VarType.STRING, super.getSystemVariables());
			logger.info(String.format("incomingFolder/toFolder:%s", incomingFolder));
		}
		this.host = super.getCfgString(cfgkey_sftp_host, null);
		this.port = super.getCfgInt(cfgkey_sftp_port, 22);
		this.user = super.getCfgString(cfgkey_sftp_user, null);
		this.pass = super.getCfgString(cfgkey_sftp_pass, null);
		this.fromDirs = super.getCfgStringArray(cfgkey_sftp_folder);
		this.recursive = super.getCfgBoolean(cfgkey_sftp_folder_recursive, false);
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
		this.outputKey = super.getCfgString(cfgkey_output_key, output_default_key);
		deleteOnly = super.getCfgBoolean(cfgkey_delete_only, false);
	}

	public List<String> process(long mapKey, String row){
		logger.info(String.format("param: %s", row));
		super.init();
		
		Map<String, String> pm = null;
		try {
			pm = ParamUtil.parseMapParams(row);
		}catch(Exception e){
			logger.error("", e);
		}
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
		if (pm.containsKey(cfgkey_sftp_folder_recursive)) {
			this.recursive = Boolean.parseBoolean(pm.get(cfgkey_sftp_folder_recursive));
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
		if (pm.containsKey(cfgkey_output_key)){
			this.outputKey = pm.get(cfgkey_output_key);
		}
		logger.info(String.format("deleteOnly flag:%b", deleteOnly));
		
		Session session = null;
		ChannelSftp sftpChannel = null;
		int sftConnectRetryCntTemp = 1;
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
			channel.connect();
			sftpChannel = (ChannelSftp) channel;
			for (String fromDir:fromDirs){
				files.addAll(
					dirCopy(sftpChannel, fromDir, incomingFolder, 0)
				);
				
				if (this.fileLimit>0 && files.size()>=this.fileLimit)
					break;
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
		}
		return files;
	}
	
	private List<String> dirCopy(ChannelSftp sftpChannel, String fromDir, String toDir, int numProcessed) throws SftpException {
		List<String> files = new ArrayList<String>();
		OutputStream fsos = null;
		InputStream is = null;
		int getRetryCntTemp = 1;
		List<String> dirCopiedFiles;
		
		if (!fromDir.endsWith(separator)){
			fromDir += separator;
		}

		if (!toDir.endsWith(separator)){
			toDir += separator;
		}
		
		logger.info(String.format("sftp folder:%s, filter:%s", fromDir, fileFilter));
		sftpChannel.cd(fromDir);
		Vector<LsEntry> v = sftpChannel.ls(fileFilter);
		
		for (LsEntry entry : v) {
			if (!entry.getAttrs().isDir()){
				String srcFile = fromDir + entry.getFilename();
				String fileNorm = entry.getFilename().replace(",", "_");
				String destFile = toDir + fileNorm;
				if (this.sftpNamesOnly){
					logger.info(String.format("find file %s", srcFile));
					files.add(srcFile);
				}else{
					logger.info(String.format("put file to %s from %s", destFile, srcFile));
					getRetryCntTemp = 1;// reset the count to 1 for every file
					if (!deleteOnly){
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
								try {
									Thread.sleep(this.sftpGetRetryWait);
								} catch (InterruptedException e1) {
									logger.error(e.getMessage(), e);
								}
							} finally {
								if (fsos != null) {
									try {
										fsos.close();
									} catch (IOException e) {
										logger.error(e.getMessage(), e);
									}
								}
								if (is != null) {
									try {
										is.close();
									} catch (IOException e) {
										logger.error(e.getMessage(), e);
									}
								}
							}
						}
					}
					// deleting file one by one if sftp.clean is enabled
					if (sftpClean) {
						logger.info("Deleting file:" + srcFile);
						try {
							sftpChannel.rm(srcFile);
						}catch(Exception e){
							logger.error("", e);
						}
					}
					files.add(destFile);
				}
				numProcessed++;
				if (this.fileLimit>0 && numProcessed>=this.fileLimit){
					logger.info(String.format("file limit %d reached, stop processing.", numProcessed));
					break;
				}
			} else if (this.recursive) {
				dirCopiedFiles=dirCopy(sftpChannel, fromDir + entry.getFilename(), toDir + entry.getFilename(), numProcessed);
				files.addAll(
					dirCopiedFiles
				);
				numProcessed+=dirCopiedFiles.size();
				if (this.fileLimit>0 && numProcessed>=this.fileLimit){
					logger.info(String.format("file limit %d reached, stop processing.", numProcessed));
					break;
				}
			}
		}
		return files;
	}
	
	@Override
	public List<String> sgProcess(){
		List<String> ret = process(0, null);
		int fileNumberTransfer=ret.size();
		List<String> logInfo = new ArrayList<String>();
		logInfo.add(fileNumberTransfer + "");
		return logInfo;
	}
	
	@Override
	public Map<String, Object> mapProcess(long offset, String row, 
			Mapper<LongWritable, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos){
		//override param
		Map<String, Object> retMap = new HashMap<String, Object>();
		List<String> files = process(offset, row);
		try {
			for (String file: files){
				if (output_default_key.equals(outputKey)){
					context.write(new Text(file), null);
				}else{
					mos.write(new Text(file), null, outputKey);
				}
			}
		}catch(Exception e){
			logger.error("", e);
		}
		List<String> logInfo = new ArrayList<String>();
		logInfo.add(String.valueOf(files.size()));
		retMap.put(ETLCmd.RESULT_KEY_LOG, logInfo);
		return retMap;
	}
	

	@Override
	public boolean hasReduce(){
		return false;
	}
	
	@Override
	public List<Tuple2<String, String>> flatMapToPair(String tableName, String value, 
			Mapper<LongWritable, Text, Text, Text>.Context context) throws Exception{
		List<String> fileList = process(0, value);
		List<Tuple2<String, String>> ret = new ArrayList<Tuple2<String, String>>();
		for (String file:fileList){
			if (output_default_key.equals(outputKey)){
				ret.add(new Tuple2<String,String>(file, null));
			}else{
				ret.add(new Tuple2<String,String>(outputKey, file));
			}
			
		}
		return ret;
	}
	/*
	 * called from sparkProcessFileToKV, key: file Name, v: line value
	 */
	@Override
	public JavaPairRDD<String, String> sparkProcessV2KV(JavaRDD<String> input, JavaSparkContext jsc, Class<? extends InputFormat> inputFormatClass){
		return input.flatMapToPair(new PairFlatMapFunction<String, String, String>(){
			@Override
			public Iterator<Tuple2<String, String>> call(String t) throws Exception {
				List<String> fileList = process(0, t);
				List<Tuple2<String, String>> ret = new ArrayList<Tuple2<String, String>>();
				for (String file:fileList){
					if (output_default_key.equals(outputKey)){
						ret.add(new Tuple2<String,String>(file, null));
					}else{
						ret.add(new Tuple2<String,String>(outputKey, file));
					}
				}
				return ret.iterator();
			}
		});
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