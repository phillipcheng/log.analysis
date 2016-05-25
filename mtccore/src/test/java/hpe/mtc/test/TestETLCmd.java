package hpe.mtc.test;

import java.io.File;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.junit.Test;

import etl.cmd.BackupCmd;
import etl.engine.ETLCmdMain;

public class TestETLCmd {
	
	public static final Logger logger = Logger.getLogger(TestETLCmd.class);
	public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	@Test
	public void testLab() throws Exception{
		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
	    ugi.doAs(new PrivilegedExceptionAction<Void>() {
	      public Void run() throws Exception {
	    	//CmdClassName wfid staticConfigFile dynamicConfigFile
	  		String wfid = sdf.format(new Date());
	  		String defaultFs = "hdfs://192.85.247.104:19000";
	  		ETLCmdMain.main(new String[]{"etl.cmd.LoadRawFileCmd", defaultFs, wfid, "/mtccore/etlcfg/sgsiwf.loadrawfile.properties", ""});
	  		ETLCmdMain.main(new String[]{"etl.cmd.dynschema.DynSchemaCmd", defaultFs, wfid, "/mtccore/etlcfg/sgsiwf.dynschema.properties", ""});
	  		ETLCmdMain.main(new String[]{"etl.cmd.SqlExecutorCmd", defaultFs, wfid, "/mtccore/etlcfg/sgsiwf.sqlexecutor.properties", "/mtccore/schemahistory/sgsiwf.dyncfg_"+wfid});
	  		ETLCmdMain.main(new String[]{"etl.cmd.BackupCmd", defaultFs, wfid, "/mtccore/etlcfg/sgsiwf.backup.properties", "/mtccore/schemahistory/sgsiwf.dyncfg_"+wfid});
			return null;
	      }
	    });
	}
	
	@Test
	public void testLocal() throws Exception{	
		//CmdClassName wfid staticConfigFile dynamicConfigFile
  		String wfid = "aaa";
  		String defaultFs = "hdfs://127.0.0.1:19000";
  		ETLCmdMain.main(new String[]{"etl.cmd.LoadRawFileCmd", defaultFs, wfid, "/mtccore/etlcfg/sgsiwf.loadrawfile.properties", ""});
  		ETLCmdMain.main(new String[]{"etl.cmd.dynschema.DynSchemaCmd", defaultFs, wfid, "/mtccore/etlcfg/sgsiwf.dynschema.properties", ""});
  		//ETLCmdMain.main(new String[]{"etl.cmd.SqlExecutorCmd", defaultFs, wfid, "/mtccore/etlcfg/sgsiwf.sqlexecutor.properties", "/mtccore/schemahistory/sgsiwf.dyncfg_"+wfid});
  		//ETLCmdMain.main(new String[]{"etl.cmd.LoadRawFileCmd", defaultFs, wfid, "/mtccore/etlcfg/sgsiwf.loadrawfile.properties", "/mtccore/schemahistory/sgsiwf.dyncfg_"+wfid});
	}
	
	@Test
	public void setupLocalETLCfg() {
		setupETLCfg("hdfs://127.0.0.1:19000", "C:\\mydoc\\myprojects\\log.analysis\\mtccore\\src\\test\\resources");
	}
	
	@Test
	public void setupLabETLCfg() {
		setupETLCfg("hdfs://192.85.247.104:19000", "C:\\mydoc\\myprojects\\log.analysis\\mtccore\\src\\test\\resources");
	}
	
	public void realSetupEtlCfg(String defaultFs, String localCfgDir) throws Exception{
		Configuration conf = new Configuration();
    	conf.set("fs.defaultFS", defaultFs);
    	FileSystem fs = FileSystem.get(conf);
		String remoteEtlcfg = "/mtccore/etlcfg";
		String csvdir = "/mtccore/csvdata";
		String schemadir = "/mtccore/schema";
		String schemaHistoryDir = "/mtccore/schemahistory";
		fs.delete(new Path(remoteEtlcfg), true);
		fs.delete(new Path(csvdir), true);
		fs.delete(new Path(schemadir), true);
		fs.delete(new Path(schemaHistoryDir), true);
		
		fs.mkdirs(new Path(schemadir));
		fs.mkdirs(new Path(csvdir));
		fs.mkdirs(new Path(schemaHistoryDir));
		File localDir = new File(localCfgDir);
		String[] cfgs = localDir.list();
		for (String cfg:cfgs){
			String lcfg = localCfgDir + File.separator + cfg;
			String rcfg = remoteEtlcfg + "/" + cfg;
			fs.copyFromLocalFile(new Path(lcfg), new Path(rcfg));
		}
	}
	
	public void setupETLCfg(final String defaultFs, final String localCfgDir) {
		try {
			if (defaultFs.contains("127.0.0.1")){
				realSetupEtlCfg(defaultFs, localCfgDir);
			}else{
				UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			    ugi.doAs(new PrivilegedExceptionAction<Void>() {
			      public Void run() throws Exception {
			    	realSetupEtlCfg(defaultFs, localCfgDir);
					return null;
			      }
			    });
			}
		}catch(Exception e){
			logger.error("", e);
		}
	}
}
