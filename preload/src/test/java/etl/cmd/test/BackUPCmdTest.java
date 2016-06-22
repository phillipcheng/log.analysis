package etl.cmd.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.conf.Configuration;

import etl.cmd.BackupCmd;
import etl.cmd.SftpCmd;
import etl.engine.InvokeMapper;
import static org.junit.Assert.*;
import org.apache.log4j.Logger;

public class BackUPCmdTest {
	public Configuration conf = new Configuration();
	public static final Logger logger = Logger.getLogger(BackUPCmdTest.class);
	String defaultFS =null;
	String wfid=null;

	/** * Initialization */
	@Before
	public void setUp() {

		wfid="0000065-162525414258816-oozie-dbad-W"; //use wfid as per necessity
	}

	@Test
	public void test1_XmlFolder() throws Exception {

		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				try {


					Configuration conf = new Configuration();
					String defaultFS = "hdfs://192.85.247.104:19000";
					conf.set("fs.defaultFS", defaultFS);
					FileSystem fs = FileSystem.get(conf);
					String localFolder = "C:\\Users\\yaligar\\git\\log.analysis\\mtccore\\src\\main\\resources\\";
					String dfsFolder = "/test_folder/";
					String cfg = "sgsiwf.backup1.properties";
					fs.copyFromLocalFile(new Path(localFolder + cfg), new Path(dfsFolder+cfg));
					BackupCmd cmd = new BackupCmd(wfid, dfsFolder+cfg, null, null, defaultFS);
					cmd.process("xml-folder=/test_folder/xmldata", null);

				} catch (Exception e) {
					// TODO: handle exception
				}
				return null;
			}
		});
	}

	@Test
	public void test1_CsvFolder() throws Exception {

		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				try {

					Configuration conf = new Configuration();
					String defaultFS = "hdfs://192.85.247.104:19000";
					conf.set("fs.defaultFS", defaultFS);
					FileSystem fs = FileSystem.get(conf);
					String localFolder = "C:\\Users\\yaligar\\git\\log.analysis\\mtccore\\src\\main\\resources\\";
					String dfsFolder = "/test_folder/";
					String cfg = "sgsiwf.backup1.properties";
					fs.copyFromLocalFile(new Path(localFolder + cfg), new Path(dfsFolder+cfg));
					BackupCmd cmd = new BackupCmd(wfid, dfsFolder+cfg, null, null, defaultFS);
					cmd.process("csv-folder=/test_folder/csvdat", null);
				} catch (Exception e) {
					// TODO: handle exception
				}
				return null;
			}
		});
	}
	@Test
	public void test1_DataHistory() throws Exception {

		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				try {

					Configuration conf = new Configuration();
					String defaultFS = "hdfs://192.85.247.104:19000";
					conf.set("fs.defaultFS", defaultFS);
					FileSystem fs = FileSystem.get(conf);
					String localFolder = "C:\\Users\\yaligar\\git\\log.analysis\\mtccore\\src\\main\\resources\\";
					String dfsFolder = "/test_folder/";
					String cfg = "sgsiwf.backup1.properties";
					fs.copyFromLocalFile(new Path(localFolder + cfg), new Path(dfsFolder+cfg));
					BackupCmd cmd = new BackupCmd(wfid, dfsFolder+cfg, null, null, defaultFS);
					cmd.process("data-history-folder=/test_folder/dataHistory/", null);
				} catch (Exception e) {
					// TODO: handle exception
				}
				return null;
			}
		});
	}

	@Test
	public void test1_destinationZipFolder() throws Exception {

		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				try {
					Configuration conf = new Configuration();
					String defaultFS = "hdfs://192.85.247.104:19000";
					conf.set("fs.defaultFS", defaultFS);
					FileSystem fs = FileSystem.get(conf);
					String localFolder = "C:\\Users\\yaligar\\git\\log.analysis\\mtccore\\src\\main\\resources\\";
					String dfsFolder = "/test_folder/";
					String cfg = "sgsiwf.backup1.properties";
					fs.copyFromLocalFile(new Path(localFolder + cfg), new Path(dfsFolder+cfg));
					BackupCmd cmd = new BackupCmd(wfid, dfsFolder+cfg, null, null, defaultFS);
					cmd.process("destination-zip-folder=/test_folder/dataHistory/"+wfid+".zip", null);
				
				} 
			catch (Exception e) {
					// TODO: handle exception
				}
				return null;
			}
		});
	}


}