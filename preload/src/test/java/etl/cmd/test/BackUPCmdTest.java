package etl.cmd.test;

import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;
import etl.cmd.BackupCmd;
import org.apache.log4j.Logger;

public class BackUPCmdTest {

	public static final Logger logger = Logger.getLogger(BackUPCmdTest.class);
	private Configuration conf;
	private String wfid=null;
	private String dynconf=null;
	private String localFolder;
	private String dfsFolder;
	private String defaultFS;
	private String cfg;
	private FileSystem fs;

	/** * Initialization */
	@Before
	public void setUp() {
		conf = new Configuration();
		defaultFS = "hdfs://192.85.247.104:19000";
		conf.set("fs.defaultFS", defaultFS);
		localFolder = "C:\\Users\\yaligar\\git\\log.analysis\\mtccore\\src\\main\\resources\\";
		dfsFolder = "/test_folder/";
		cfg = "sgsiwf.backup1.properties";
		wfid="0000065-162525414258816-oozie-dbad-W"; //use wfid as per necessity
		dynconf="sgsiwf.dyncfg_0000085-160525114258816-oozie-dbad-W";
	}

	@Test
	// Testing path of data-history-folder.
	public void test1_DataHistory() throws Exception {

		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				try {
					fs = FileSystem.get(conf);
					fs.copyFromLocalFile(new Path(localFolder + cfg), new Path(dfsFolder+cfg));
					BackupCmd cmd = new BackupCmd(wfid, dfsFolder+cfg, dynconf, null, defaultFS);
					cmd.process("data-history-folder=/test_folder/dataHistory/", null);
				} catch (Exception e) {
					logger.error("Exception occured due to invalid data-history path", e);
				}
				return null;
			}
		});
	}

	@Test
	// Testing with destination-Zip-Folder.
	public void test2_destinationZipFolder() throws Exception {

		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				try {
					fs = FileSystem.get(conf);
					fs.copyFromLocalFile(new Path(localFolder + cfg), new Path(dfsFolder+cfg));
					BackupCmd cmd = new BackupCmd(wfid, dfsFolder+cfg, dynconf, null, defaultFS);
					cmd.process("destination-zip-folder=/test_folder/dataHistory/"+wfid+".zip", null);
				} 
				catch (Exception e) {
					// TODO: handle exception
					logger.error("Exception occured due to invalid destination Zip-folder path ", e);
				}
				return null;
			}
		});
	}



	@Test
	// Testing with default folder.filter and default file filter.
	public void test3_default_folderfilter_default_fileFilter() throws Exception {

		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				try {
					fs = FileSystem.get(conf);
					fs.copyFromLocalFile(new Path(localFolder + cfg), new Path(dfsFolder+cfg));
					BackupCmd cmd = new BackupCmd(wfid, dfsFolder+cfg, dynconf, null, defaultFS);
					cmd.process(null, null);
				} catch (Exception e) {
					logger.error("", e);
				}
				return null;
			}
		});
	}

	@Test
	// Testing with "xml" folder.filter and raw.xml.files file filter.
	public void test4_xmlFolder_raw_Xml_fileFilter() throws Exception {

		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				try {
					fs = FileSystem.get(conf);
					fs.copyFromLocalFile(new Path(localFolder + cfg), new Path(dfsFolder+cfg));
					BackupCmd cmd = new BackupCmd(wfid, dfsFolder+cfg, dynconf, null, defaultFS);
					cmd.process("folder.filter=/test_folder/dataHistory/datahistory/ ,file.filter=raw.xml.files", null);
				} catch (Exception e) {
					logger.error("", e);
				}
				return null;
			}
		});
	}

	@Test
	// Testing with folder.filter csv-folder and WFID file filter.
	public void test5_csvfolder_WFID_fileFilter() throws Exception {

		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				try {
					fs = FileSystem.get(conf);
					fs.copyFromLocalFile(new Path(localFolder + cfg), new Path(dfsFolder+cfg));
					BackupCmd cmd = new BackupCmd(wfid, dfsFolder+cfg, dynconf, null, defaultFS);
					cmd.process("folder.filter=/test_folder/csvdata/,file.filter=WFID", null);
				} catch (Exception e) {
					logger.error("", e);
				}
				return null;
			}
		});
	}


	@Test
	// Testing with New folder as folder.filter  and ALL file filter.
	public void test6_New_folder_ALL_fileFilter() throws Exception {

		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				try {
					fs = FileSystem.get(conf);
					fs.copyFromLocalFile(new Path(localFolder + cfg), new Path(dfsFolder+cfg));
					BackupCmd cmd = new BackupCmd(wfid, dfsFolder+cfg, dynconf, null, defaultFS);
					cmd.process("folder.filter=/test_folder/datafolder3,file.filter=ALL", null);
				} catch (Exception e) {
					logger.error("", e);
				}
				return null;
			}
		});
	}



	@Test
	// Testing with xml and csv folder.filter and raw.xml.files and WFID file filter.
	public void test7_Xml_Csv_raw_WFID_fileFilter() throws Exception {

		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				try {
					fs = FileSystem.get(conf);
					fs.copyFromLocalFile(new Path(localFolder + cfg), new Path(dfsFolder+cfg));
					BackupCmd cmd = new BackupCmd(wfid, dfsFolder+cfg, dynconf, null, defaultFS);
					cmd.process("folder.filter=/test_folder/dataHistory/datahistory/ /test_folder/csvdata/ ,file.filter=raw.xml.files WFID", null);
				} catch (Exception e) {
					logger.error("", e);
				}
				return null;
			}
		});
	}
	@Test
	// Testing with New folder.filter and ALL file filter.
	public void test8_additional_folder_ALL_fileFilter() throws Exception {

		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				try {
					fs = FileSystem.get(conf);
					fs.copyFromLocalFile(new Path(localFolder + cfg), new Path(dfsFolder+cfg));
					BackupCmd cmd = new BackupCmd(wfid, dfsFolder+cfg, dynconf, null, defaultFS);
					cmd.process("folder.filter=/test_folder/dataHistory/datahistory/ /test_folder/csvdata/ /test_folder/datafolder3/,file.filter=raw.xml.files WFID ALL", null);
				} catch (Exception e) {
					logger.error("", e);
				}
				return null;
			}
		});
	}


}