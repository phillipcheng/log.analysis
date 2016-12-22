package bdap.tools.pushagent;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.FalseFileFilter;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.MapContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.CronScheduleBuilder;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.ChannelSftp.LsEntry;

@DisallowConcurrentExecution
public class Main implements Job {
	private static final Logger logger = LogManager.getLogger(Main.class);
	private static final JexlEngine jexl = new JexlBuilder().cache(512).strict(true).silent(false).create();
	private static final String PUSH_AGENT_GROUP = "PushAgent";
	private static final String DEST_SERVER = "DestServer";
	private static final String DEST_SERVER_PORT = "DestServerPort";
	private static final String DEST_SERVER_USER = "DestServerUser";
	private static final String DEST_SERVER_PRVKEY = "DestServerPrvKey";
	private static final String DEST_SERVER_PASS = "DestServerPass";
	private static final String DEST_SERVER_DIR_RULE = "DestServerDirRule";
	private static final String WORKING_ELEMENT = "WorkingElement";
	private static final String WORKING_DIR = "WorkingDir";
	private static final String FILENAME_FILTER = "FilenameFilter";
	private static final String FILES_PER_BATCH = "FilesPerBatch";
	private static final String RECURSIVE = "Recursive";
	private static final String UNIX_SEPARATOR = "/";
	private static final String PROCESS_RECORD = "ProcessRecord";
	private int sftpRetryCount = 3;
	private int sftpRetryWait = 15000;

	public void execute(JobExecutionContext context) throws JobExecutionException {
		JobDataMap ctxMap = context.getMergedJobDataMap();
		logger.info("Execute by trigger: {}", context.getTrigger().getKey());
		
		String destDirRule = ctxMap.getString(DEST_SERVER_DIR_RULE);
		
		if (destDirRule != null) {
			JexlExpression e = jexl.createExpression(destDirRule);
			JexlContext jexlCtx = new MapContext(ctxMap);
			Object destDir = e.evaluate(jexlCtx);
			
			FileInputStream in = null;
			String processRecordFile = ctxMap.getString(PROCESS_RECORD);
			String text;
			FileRecord fileRecord;
			if (processRecordFile != null && processRecordFile.length() > 0 && new File(processRecordFile).exists()) {
				try {
					in = new FileInputStream(processRecordFile);
					text = IOUtils.toString(in, StandardCharsets.UTF_8);
					fileRecord = fromJsonString(text, FileRecord.class);
				} catch (Exception e1) {
					logger.error(e1.getMessage(), e1);
					fileRecord = new FileRecord();
				} finally {
					if (in != null)
						try {
							in.close();
						} catch (IOException e1) {
							logger.error(e1.getMessage(), e1);
						}
				}
			} else {
				fileRecord = new FileRecord();
			}
			
			sftpCopy(ctxMap.getString(WORKING_DIR), (IOFileFilter) ctxMap.get(FILENAME_FILTER), ctxMap.getBoolean(RECURSIVE),
					ctxMap.getInt(FILES_PER_BATCH), fileRecord, processRecordFile, ctxMap.getString(DEST_SERVER),
					ctxMap.getInt(DEST_SERVER_PORT), ctxMap.getString(DEST_SERVER_USER), ctxMap.getString(DEST_SERVER_PRVKEY),
					ctxMap.getString(DEST_SERVER_PASS), (String) destDir);
		} else {
			logger.error("No dest dir rule set!");
		}
	}

	private void sftpCopy(String srcDir, IOFileFilter filenameFilter, boolean recursive, int maxFiles, FileRecord fileRecord, String processRecordFile,
			String destServer, int destServerPort, String destServerUser, String destServerPrvKey, String destServerPass, String destDir) {
		Session session = null;
		ChannelSftp sftpChannel = null;
		int sftConnectRetryCntTemp = 1;
		try {
			// connect
			JSch jsch = new JSch();
			
			if (destServerPrvKey != null && destServerPrvKey.length() > 0) {
				if (destServerPass != null && destServerPass.length() > 0)
					jsch.addIdentity(destServerPrvKey, destServerPass);
				else
					jsch.addIdentity(destServerPrvKey);
			}
			
			Channel channel = null;
			session = jsch.getSession(destServerUser, destServer, destServerPort);
			session.setConfig("StrictHostKeyChecking", "no");
			
			if (destServerPrvKey == null || destServerPrvKey.length() == 0)
				session.setPassword(destServerPass);

			// retry for session connect
			while (sftConnectRetryCntTemp <= sftpRetryCount) {
				try {
					session.setTimeout(this.sftpRetryWait);
					session.connect();
					channel = session.openChannel("sftp");
					break;
				} catch (Exception e) {
					if (sftConnectRetryCntTemp == sftpRetryCount) {
						logger.error("Reached maximum number of times for connecting session.");
						throw new SftpException(0, String.format(
								"Reached maximum number of times for connecting sftp session to %s", destServerUser));
					}
					logger.error("Session connection failed. retrying..." + sftConnectRetryCntTemp);
					sftConnectRetryCntTemp++;
				}
			}

			channel.connect();
			sftpChannel = (ChannelSftp) channel;
			Collection<File> v;

			if (recursive)
				v = FileUtils.listFiles(new File(srcDir), filenameFilter, TrueFileFilter.INSTANCE);
			else
				v = FileUtils.listFiles(new File(srcDir), filenameFilter, FalseFileFilter.INSTANCE);
			
			/* Avoid duplicate copy the file */
			v = filterProcessedFiles(v, fileRecord);
			Collections.sort((List<File>)v, new FileAgeComparator());

			logger.info("source folder: {}, filter: {}", srcDir, filenameFilter);

			mkdirs(sftpChannel, destDir);

			sftpChannel.cd(destDir);
			
			logger.info("destination folder: {}", destDir);

			String relativePath;
			String relativeDir;
			FileInputStream in;
			int filesCopied = 0;
			
			boolean flag = false;

			for (File entry : v) {
				if (!flag && entry.lastModified() > fileRecord.getTimestamp())
					/* file is later than last record time */
					flag = true;
				
				if (!flag && entry.lastModified() == fileRecord.getTimestamp() && entry.getAbsolutePath().equals(fileRecord.getFilePath()))
					/* Find the last record file, skip it and continue */
					flag = true;
				
				else if (flag) {
					relativeDir = new File(srcDir).toURI().relativize(entry.getParentFile().toURI()).getPath();
					relativePath = new File(srcDir).toURI().relativize(entry.toURI()).getPath();
					
					if (!relativeDir.startsWith("."))
						relativeDir = "." + UNIX_SEPARATOR + relativeDir;
	
					mkdirs(sftpChannel, relativeDir);
					
					in = null;
	
					try {
						in = new FileInputStream(entry);
						sftpChannel.put(in, relativePath, ChannelSftp.OVERWRITE);
						writeProcessRecord(processRecordFile, fileRecord, entry);
						filesCopied ++;
						logger.info("put the file: {}", relativePath);
						
					} finally {
						if (in != null)
							in.close();
					}
					
					if (maxFiles != 0 && filesCopied >= maxFiles)
						/* Exceed the max files per batch */
						break;
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
	}
	
	private Collection<File> filterProcessedFiles(Collection<File> files, FileRecord fileRecord) {
		ArrayList<File> newList = new ArrayList<File>();
		for (File f: files) {
			if (f.lastModified() >= fileRecord.getTimestamp())
				newList.add(f);
		}
		return newList;
	}

	private void writeProcessRecord(String processRecordFile, FileRecord fileRecord, File entry) throws Exception {
		if (processRecordFile != null && processRecordFile.length() > 0) {
			fileRecord.setFilePath(entry.getAbsolutePath());
			fileRecord.setTimestamp(entry.lastModified());
			String text = toJsonString(fileRecord);
			OutputStream output = null;
			try {
				output = new FileOutputStream(processRecordFile);
				IOUtils.write(text, output, StandardCharsets.UTF_8);
			} finally {
				if (output != null)
					output.close();
			}
		}
	}

	private void mkdirs(ChannelSftp ch, String destDir) {
		try {
			String[] folders = destDir.split(UNIX_SEPARATOR);
			if (folders[0].isEmpty() || ".".equals(folders[0]))
				folders[0] += UNIX_SEPARATOR;
			String fullPath = folders[0];
			for (int i = 1; i < folders.length; i++) {
				Vector<LsEntry> ls = ch.ls(fullPath);
				boolean isExist = false;
				for (LsEntry e : ls) {
					if (e.getAttrs().isDir() && e.getFilename().equals(folders[i])) {
						isExist = true;
					}
				}
				if (!isExist && !folders[i].isEmpty()) {
					ch.mkdir(fullPath + folders[i]);
				}
				fullPath = fullPath + folders[i] + UNIX_SEPARATOR;
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	public static void main(String[] args) throws Exception {
		String configText = "";
		InputStream in;

		if (args.length > 0) {
			/* Read the config file from URL */
			in = new URL(args[0]).openStream();
		} else {
			/* Read the default config file from class path */
			in = Main.class.getResourceAsStream("/default-config.json");
		}

		try {
			configText = IOUtils.toString(in, StandardCharsets.UTF_8);
		} finally {
			IOUtils.closeQuietly(in);
		}
		
		Config config = fromJsonString(configText, Config.class);
		
		if (config != null && !config.isEmpty()) {
			// Grab the Scheduler instance from the Factory
			final Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
	
			// and start it off
			scheduler.start();
	
			JobDetail job = JobBuilder.newJob(Main.class).withIdentity("MainJob", PUSH_AGENT_GROUP).storeDurably().build();
			Trigger trigger;
			Element e;
			
			scheduler.addJob(job, true);
			
			for (DirConfig dc: config.values()) {
				if ((e = currentElement(dc.getElements())) != null) {
					trigger = TriggerBuilder.newTrigger().withIdentity(dc.getId() + "Trigger", PUSH_AGENT_GROUP)
							.withSchedule(CronScheduleBuilder.cronSchedule(dc.getCronExpr()).inTimeZone(TimeZone.getTimeZone(dc.getTimeZone())))
							.forJob(job).build();
					
					trigger.getJobDataMap().put(WORKING_ELEMENT, e);
					trigger.getJobDataMap().put(WORKING_DIR, dc.getDirectory());
					trigger.getJobDataMap().put(RECURSIVE, dc.isRecursive());
					trigger.getJobDataMap().put(FILES_PER_BATCH, dc.getFilesPerBatch());
					trigger.getJobDataMap().put(PROCESS_RECORD, dc.getProcessRecordFile());
					trigger.getJobDataMap().put(DEST_SERVER, dc.getDestServer());
					trigger.getJobDataMap().put(DEST_SERVER_PORT, dc.getDestServerPort());
					trigger.getJobDataMap().put(DEST_SERVER_USER, dc.getDestServerUser());
					trigger.getJobDataMap().put(DEST_SERVER_PRVKEY, dc.getDestServerPrvKey());
					trigger.getJobDataMap().put(DEST_SERVER_PASS, dc.getDestServerPass());
					trigger.getJobDataMap().put(DEST_SERVER_DIR_RULE, dc.getDestServerDirRule());
					trigger.getJobDataMap().put(FILENAME_FILTER, createIOFileFilter(dc.getFilenameFilterExpr(), trigger.getJobDataMap()));
			
					scheduler.scheduleJob(trigger);
				}
			}
	
			Thread appThread = new Thread(new Runnable() {
				public void run() {
					try {
						while (existsJob(scheduler)) { // run forever
							Thread.sleep(1000);
						}
	
					} catch (Exception e) {
						logger.error(e.getMessage(), e);
					} finally {
						try {
							scheduler.shutdown();
						} catch (SchedulerException e) {
							logger.error(e.getMessage(), e);
						}
					}
				}
			});
	
			appThread.setDaemon(true);
			appThread.start();
		}
	}

	private static IOFileFilter createIOFileFilter(String filterExpr, Map<String, Object> currentCtx) {
		if (filterExpr != null) {
			JexlExpression e = jexl.createExpression(filterExpr);
			JexlContext context = new MapContext(currentCtx);
			Object obj = e.evaluate(context);
			if (obj != null && obj instanceof IOFileFilter)
				return (IOFileFilter) obj;
			else /* Wrong expression will return false filter */
				return FalseFileFilter.INSTANCE;
		} else {
			return TrueFileFilter.INSTANCE;
		}
	}

	private static Element currentElement(Element[] elements) throws SocketException {
		if (elements != null) {
			for (Element e: elements) {
				if (e.getIp() != null) {
					/* Current server IP */
					Enumeration<NetworkInterface> nis = NetworkInterface.getNetworkInterfaces();
					Enumeration<InetAddress> ias;
					while (nis.hasMoreElements()) {
						ias = nis.nextElement().getInetAddresses();
						while (ias.hasMoreElements())
							if (e.getIp().equals(ias.nextElement().getHostAddress()))
								return e;
					}
				}
			}
		}
		return null;
	}

	private static boolean existsJob(Scheduler scheduler) throws Exception {
		for (String group : scheduler.getJobGroupNames()) {
			// enumerate each job in group
			GroupMatcher<JobKey> m = GroupMatcher.groupEquals(group);
			if (scheduler.getJobKeys(m).iterator().hasNext())
				return true;
		}
		return false;
	}
	
	private static <T> T fromJsonString(String json, Class<T> clazz){
		return fromJsonString(json, clazz, false);
	}
	
	private static <T> T fromJsonString(String json, Class<T> clazz, boolean useDefaultTyping){
		ObjectMapper mapper = new ObjectMapper();
		if (useDefaultTyping){
			mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
		}
		mapper.setSerializationInclusion(Include.NON_NULL);
		try {
			return mapper.readValue(json, clazz);
		} catch (Exception e) {
			logger.error("", e);
			return null;
		}
	}
	
	private static String toJsonString(Object ls){
		return toJsonString(ls, false);
	}
	
	private static String toJsonString(Object ls, boolean useDefaultTyping){
		ObjectMapper mapper = new ObjectMapper();
		if (useDefaultTyping){
			mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
		}
		mapper.setSerializationInclusion(Include.NON_NULL);
		ObjectWriter ow = mapper.writer().withDefaultPrettyPrinter();
		try {
			String json = ow.writeValueAsString(ls);
			return json;
		} catch (JsonProcessingException e) {
			logger.error("",e );
			return null;
		}
	}
}
