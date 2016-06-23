package etl.cmd;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import etl.engine.ETLCmd;
import etl.util.Util;

public class SftpCmd extends ETLCmd {
	public static final Logger logger = Logger.getLogger(SftpCmd.class);

	public static final String cfgkey_incoming_folder = "incoming.folder";
	public static final String cfgkey_sftp_host = "sftp.host";
	public static final String cfgkey_sftp_port = "sftp.port";
	public static final String cfgkey_sftp_user = "sftp.user";
	public static final String cfgkey_sftp_pass = "sftp.pass";
	public static final String cfgkey_sftp_folder = "sftp.folder";
	public static final String cfgkey_sftp_retry = "sftp.getRetryTimes";
	public static final String cfgkey_sftp_connect_retry = "sftp.connectRetryTimes";
	public static final String cfgkey_sftp_clean = "sftp.clean";

	private String incomingFolder;
	private String host;
	private int port;
	private String user;
	private String pass;
	private String fromDir;
	private int sftpRetryCount;
	private int sftpConnectRetryCount;
	private boolean sftpClean;

	public SftpCmd(String wfid, String staticCfg, String inDynCfg, String outDynCfg, String defaultFs) {
		super(wfid, staticCfg, inDynCfg, outDynCfg, defaultFs);
		this.incomingFolder = pc.getString(cfgkey_incoming_folder);
		this.host = pc.getString(cfgkey_sftp_host);
		this.port = pc.getInt(cfgkey_sftp_port);
		this.user = pc.getString(cfgkey_sftp_user);
		this.pass = pc.getString(cfgkey_sftp_pass);
		this.fromDir = pc.getString(cfgkey_sftp_folder);
		this.sftpRetryCount = pc.getInt(cfgkey_sftp_retry);
		this.sftpConnectRetryCount = pc.getInt(cfgkey_sftp_connect_retry);
		this.sftpClean = pc.getBoolean(cfgkey_sftp_clean);
	}

	@Override
	public List<String> process(String param, Mapper<Object, Text, Text, NullWritable>.Context context) {
		Session session = null;
		ChannelSftp sftpChannel = null;
		int retryCntTemp = 1;
		int sftConnectRetryCntTemp = 1;
		OutputStream fsos = null;
		InputStream is = null;
		try {
			// override param
			logger.info(String.format("param:", param));
			Map<String, String> pm = Util.parseMapParams(param);
			if (pm.containsKey(cfgkey_sftp_host)) {
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
				this.fromDir = pm.get(cfgkey_sftp_folder);
			}
			if (pm.containsKey(cfgkey_sftp_retry)) {
				this.sftpRetryCount = Integer.parseInt(pm.get(cfgkey_sftp_retry));
			}
			if (pm.containsKey(cfgkey_sftp_connect_retry)) {
				this.sftpConnectRetryCount = Integer.parseInt(pm.get(cfgkey_sftp_connect_retry));
			}
			if (pm.containsKey(cfgkey_sftp_clean)) {
				sftpClean = new Boolean(pm.get(cfgkey_sftp_clean));
			}
			if (pm.containsKey(cfgkey_incoming_folder)) {
				incomingFolder = pm.get(cfgkey_incoming_folder);
			}

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
					Thread.sleep(60000L);
					sftConnectRetryCntTemp++;
					logger.error("Session is not connected.hence retrying..." + sftConnectRetryCntTemp);
				}
			}

			channel.connect();
			sftpChannel = (ChannelSftp) channel;
			logger.info("From Dir:" + fromDir);
			sftpChannel.cd(fromDir);
			Vector<LsEntry> v = sftpChannel.ls("*");
			for (LsEntry entry : v) {
				String srcFile = fromDir + entry.getFilename();
				String destFile = incomingFolder + entry.getFilename();
				logger.info(String.format("get file from %s to %s", srcFile, destFile));

				retryCntTemp = 1;// reset the count to 1 for every file
				while (retryCntTemp <= sftpRetryCount) {
					try {
						fsos = fs.create(new Path(destFile));
						is = sftpChannel.get(srcFile);
						IOUtils.copy(is, fsos);
						break;
					} catch (Exception e) {
						logger.error("Exception during transferring the file.", e);
						if (retryCntTemp == sftpRetryCount) {
							logger.info("Copying" + srcFile + "failed Retried for maximum times");
							break;
						}
						retryCntTemp++;
						logger.info("Copying" + srcFile + "failed,Retrying..." + retryCntTemp);
						Thread.sleep(60000L);
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
		return null;
	}
}