package hpe.mtccore.test;

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

import etl.engine.ETLCmdMain;

public class MtcETLCmd {
	
	public static final Logger logger = Logger.getLogger(MtcETLCmd.class);
	public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	@Test
	public void testLab() throws Exception{
		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
	    ugi.doAs(new PrivilegedExceptionAction<Void>() {
	      public Void run() throws Exception {
	    	String wfid = sdf.format(new Date());
	  		String dynCfg = "/mtccore/schemahistory/sgsiwf.dyncfg_"+wfid;
	  		String defaultFs = "hdfs://192.85.247.104:19000";
	  		ETLCmdMain.main(new String[]{"etl.cmd.UploadCmd", wfid, 
	  				"/mtccore/etlcfg/sgsiwf.upload.properties", "unused", "unused", defaultFs});
	  		ETLCmdMain.main(new String[]{"etl.cmd.dynschema.DynSchemaCmd", wfid, 
	  				"/mtccore/etlcfg/sgsiwf.dynschema.properties", "unused", dynCfg, defaultFs});
	  		ETLCmdMain.main(new String[]{"etl.cmd.SqlExecutorCmd", wfid, 
	  				"/mtccore/etlcfg/sgsiwf.sqlexecutor.properties", dynCfg, "unused", defaultFs});
	  		ETLCmdMain.main(new String[]{"etl.cmd.BackupCmd", wfid, 
	  				"/mtccore/etlcfg/sgsiwf.backup.properties", dynCfg, "unused", defaultFs});
			return null;
	      }
	    });
	}
	
	//step by step
	@Test
	public void setupLabWorkflow() {
		setupETLCfg("hdfs://192.85.247.104:19000", "C:\\Users\\yaligar\\git\\log.analysis\\mtccore\\src\\main\\resources");
	}
	
	public void realSetupWorkflow(String defaultFs, String localCfgDir) throws Exception{
		Configuration conf = new Configuration();
    	conf.set("fs.defaultFS", defaultFs);
    	FileSystem fs = FileSystem.get(conf);
		String workflow = localCfgDir + File.separator + "workflow.xml";
		String remoteWorkflow = "/user/dbadmin/mtccore/workflow.xml";
		fs.copyFromLocalFile(new Path(workflow), new Path(remoteWorkflow));
	}
	
	
	//for whole flow
	@Test
	public void setupLabETLCfg() {
		setupETLCfg("hdfs://192.85.247.104:19000", "C:\\Users\\yaligar\\git\\log.analysis\\mtccore\\src\\main\\resources");
	}
	
	public void realSetupEtlCfg(String defaultFs, String localCfgDir) throws Exception{
		Configuration conf = new Configuration();
    	conf.set("fs.defaultFS", defaultFs);
    	FileSystem fs = FileSystem.get(conf);
		String remoteEtlcfg = "/mtccore/etlcfg";
		String xmldir = "/mtccore/xmldata";
		String csvdir = "/mtccore/csvdata";
		String schemadir = "/mtccore/schema";
		String schemaHistoryDir = "/mtccore/schemahistory";
		fs.delete(new Path(remoteEtlcfg), true);
		fs.delete(new Path(csvdir), true);
		fs.delete(new Path(xmldir), true);
		fs.delete(new Path(schemadir), true);
		fs.delete(new Path(schemaHistoryDir), true);
		
		fs.mkdirs(new Path(csvdir));
		fs.mkdirs(new Path(xmldir));
		fs.mkdirs(new Path(schemadir));
		fs.mkdirs(new Path(schemaHistoryDir));
		File localDir = new File(localCfgDir);
		String[] cfgs = localDir.list();
		for (String cfg:cfgs){
			String lcfg = localCfgDir + File.separator + cfg;
			String rcfg = remoteEtlcfg + "/" + cfg;
			fs.copyFromLocalFile(new Path(lcfg), new Path(rcfg));
		}
		String workflow = localCfgDir + File.separator + "workflow.xml";
		String remoteWorkflow = "/user/dbadmin/mtccore/workflow.xml";
		fs.copyFromLocalFile(new Path(workflow), new Path(remoteWorkflow));
		//for coordinator job.
		String coordinator = localCfgDir + File.separator + "coordinator.xml";
		String remoteCoordinatorXml  = "/user/dbadmin/mtccore/coordinator.xml";
		fs.copyFromLocalFile(new Path(coordinator), new Path(remoteCoordinatorXml));
		String coordinatorProperties = localCfgDir + File.separator + "coordinatorjob.properties";
		String remoteCoordinatorProperties  = "/user/dbadmin/mtccore/coordinatorjob.properties";
		fs.copyFromLocalFile(new Path(coordinatorProperties), new Path(remoteCoordinatorProperties));
		String localTargetFolder = "C:\\Users\\yaligar\\git\\log.analysis\\mtccore\\target\\";
		String localLibFolder = "C:\\Users\\yaligar\\git\\log.analysis\\mtccore\\lib\\";
		String libName = "mtccore-0.1.0-jar-with-dependencies.jar";
		String verticaLibName = "vertica-jdbc-7.0.1-0.jar";
		String remoteLibFolder="/user/dbadmin/mtccore/lib/";
		fs.copyFromLocalFile(new Path(localTargetFolder + libName), new Path(remoteLibFolder+libName));
		fs.copyFromLocalFile(new Path(localLibFolder + verticaLibName), new Path(remoteLibFolder+verticaLibName));
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
