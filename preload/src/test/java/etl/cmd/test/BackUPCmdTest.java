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
	    cfg = "sgsiwf.backup.properties";
		wfid="0000065-162525414258816-oozie-dbad-W"; //use wfid as per necessity
		dynconf="/mtccore/schemahistory/sgsiwf.dyncfg_0000083-160525114258816-oozie-dbad-W";
	}

	@Test 
	// Testing of xml-folder [ Not a folder filter ].
	public void test1_XmlFolder() throws Exception {

		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				try {
					fs = FileSystem.get(conf);
					fs.copyFromLocalFile(new Path(localFolder + cfg), new Path(dfsFolder+cfg));
					BackupCmd cmd = new BackupCmd(wfid, dfsFolder+cfg, null, null, defaultFS);
					cmd.process("xml-folder=/test_folder/xmldata/", null);

				} catch (Exception e) {
					logger.error("", e);
				}
				return null;
			}
		});
	}
	@Test 
	// Testing with  csv-folder [ Not a folder filter ].
	public void test2_CsvFolder() throws Exception {

		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				try {
					fs = FileSystem.get(conf);
					fs.copyFromLocalFile(new Path(localFolder + cfg), new Path(dfsFolder+cfg));
					BackupCmd cmd = new BackupCmd(wfid, dfsFolder+cfg, null, null, defaultFS);
					cmd.process("csv-folder=/test_folder/csvdata/", null);

				} catch (Exception e) {
					logger.error("", e);
				}
				return null;
			}
		});
	}

	@Test
	// Testing with data-history-folder.
	public void test3_DataHistory() throws Exception {

		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				try {
					fs = FileSystem.get(conf);
					fs.copyFromLocalFile(new Path(localFolder + cfg), new Path(dfsFolder+cfg));
					BackupCmd cmd = new BackupCmd(wfid, dfsFolder+cfg, null, null, defaultFS);
					cmd.process("data-history-folder=/test_folder/dataHistory/", null);
				    } catch (Exception e) {
					logger.error("", e);
				}
				return null;
			}
		});
	}

	@Test
	// Testing with destination-Zip-Folder.
	public void test4_destinationZipFolder() throws Exception {

		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				try {
					fs = FileSystem.get(conf);
					fs.copyFromLocalFile(new Path(localFolder + cfg), new Path(dfsFolder+cfg));
					BackupCmd cmd = new BackupCmd(wfid, dfsFolder+cfg, null, null, defaultFS);
					cmd.process("destination-zip-folder=/test_folder/dataHistory/"+wfid+".zip", null);
				} 
			catch (Exception e) {
					// TODO: handle exception
				logger.error("", e);
				}
				return null;
			}
		});
	}

	@Test
	// Testing with folder.filter xml-folder and default file filter.
	public void test5_xmlfolder_defaultDynamicFilter() throws Exception {
		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				try {
					fs = FileSystem.get(conf);
					fs.copyFromLocalFile(new Path(localFolder + cfg), new Path(dfsFolder+cfg));
					BackupCmd cmd = new BackupCmd(wfid, dfsFolder+cfg, null, null, defaultFS);
					cmd.process("folder.filter=/test_folder/xmldata/", null);
				} 
			catch (Exception e) {
					// TODO: handle exception
				logger.error("", e);
				}
				return null;
			}
		});
	}
	
	@Test
	// Testing with folder.filter xml-folder and WFID file filter.
	public void test6_xmlfolder_WFID_fileFilter() throws Exception {
		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				try {
					fs = FileSystem.get(conf);
					fs.copyFromLocalFile(new Path(localFolder + cfg), new Path(dfsFolder+cfg));
					BackupCmd cmd = new BackupCmd(wfid, dfsFolder+cfg, null, null, defaultFS);
					cmd.process("folder.filter=/test_folder/xmldata/,file.filter=WFID", null);
				} 
			catch (Exception e) {
					// TODO: handle exception
				logger.error("", e);
				}
				return null;
			}
		});
	}
	
	@Test
	// Testing with folder.filter xml-folder and ALL file filter.
	public void test7_xmlfolder_ALL_fileFilter() throws Exception {
		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				try {
					fs = FileSystem.get(conf);
					fs.copyFromLocalFile(new Path(localFolder + cfg), new Path(dfsFolder+cfg));
					BackupCmd cmd = new BackupCmd(wfid, dfsFolder+cfg, null, null, defaultFS);
					cmd.process("folder.filter=/test_folder/xmldata/,file.filter=ALL", null);
				} 
			catch (Exception e) {
					// TODO: handle exception
				logger.error("", e);
				}
				return null;
			}
		});
	}
	
	@Test
	// Testing with folder.filter csv-folder and default file filter.
	public void test7_csvfolder_defaultfileFilter() throws Exception {

		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				try {
					fs = FileSystem.get(conf);
					fs.copyFromLocalFile(new Path(localFolder + cfg), new Path(dfsFolder+cfg));
					BackupCmd cmd = new BackupCmd(wfid, dfsFolder+cfg, null, null, defaultFS);
					cmd.process("folder.filter=/test_folder/csvdata/", null);
				} 
			catch (Exception e) {
					// TODO: handle exception
				logger.error("", e);
				}
				return null;
			}
		});
	}
	
	
	@Test
	// Testing with folder.filter csv-folder and WFID file filter.
	public void test8_csvfolder_WFID_fileFilter() throws Exception {

		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				try {
					fs = FileSystem.get(conf);
					fs.copyFromLocalFile(new Path(localFolder + cfg), new Path(dfsFolder+cfg));
					BackupCmd cmd = new BackupCmd(wfid, dfsFolder+cfg, null, null, defaultFS);
					cmd.process("folder.filter=/test_folder/csvdata/,file.filter=WFID", null);
				} catch (Exception e) {
					logger.error("", e);
				}
				return null;
			}
		});
	}
	@Test
	// Testing with folder.filter csv-folder and ALL file filter.
	public void test9_csvfolder_ALL_fileFilter() throws Exception {

		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				try {
					fs = FileSystem.get(conf);
					fs.copyFromLocalFile(new Path(localFolder + cfg), new Path(dfsFolder+cfg));
					BackupCmd cmd = new BackupCmd(wfid, dfsFolder+cfg, null, null, defaultFS);
					cmd.process("folder.filter=/test_folder/csvdata/,file.filter=ALL", null);
				} catch (Exception e) {
					logger.error("", e);
				}
				return null;
			}
		});
	}
	@Test
	// Testing with folder.filter default and ALL file filter.
	public void test10_default_folderfilter_ALL_fileFilter() throws Exception {

		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				try {
					fs = FileSystem.get(conf);
					fs.copyFromLocalFile(new Path(localFolder + cfg), new Path(dfsFolder+cfg));
					BackupCmd cmd = new BackupCmd(wfid, dfsFolder+cfg, null, null, defaultFS);
					cmd.process("file.filter=ALL", null);
				} catch (Exception e) {
					logger.error("", e);
				}
				return null;
			}
		});
	}
	@Test
	// Testing with New folder as folder.filter  and WFID file filter.
	public void test11_New_folder_WFID_fileFilter() throws Exception {

		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				try {
					fs = FileSystem.get(conf);
					fs.copyFromLocalFile(new Path(localFolder + cfg), new Path(dfsFolder+cfg));
					BackupCmd cmd = new BackupCmd(wfid, dfsFolder+cfg, null, null, defaultFS);
					cmd.process("folder.filter=/test_folder/datafolder3,file.filter=WFID", null);
				} catch (Exception e) {
					logger.error("", e);
				}
				return null;
			}
		});
	}
	
	@Test
	// Testing with New folder.filter and ALL file filter.
	public void test11_New_folder_default_fileFilter() throws Exception {

		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				try {
					fs = FileSystem.get(conf);
					fs.copyFromLocalFile(new Path(localFolder + cfg), new Path(dfsFolder+cfg));
					BackupCmd cmd = new BackupCmd(wfid, dfsFolder+cfg, null, null, defaultFS);
					cmd.process("folder.filter=/test_folder/datafolder3,file.filter=ALL", null);
				} catch (Exception e) {
					logger.error("", e);
				}
				return null;
			}
		});
	}

	
	
	
}