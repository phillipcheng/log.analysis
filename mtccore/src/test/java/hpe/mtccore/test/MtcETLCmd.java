package hpe.mtccore.test;

import java.io.File;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import etl.cmd.test.TestETLCmd;

public class MtcETLCmd extends TestETLCmd{
	
	public static final Logger logger = Logger.getLogger(MtcETLCmd.class);
	public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	
	@Test
	public void setupLabETLCfg() {
		setupETLCfg("hdfs://192.85.247.104:19000", "C:\\mydoc\\myprojects\\log.analysis\\mtccore\\src\\main\\resources");
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
		String[] workflows = new String[]{"sgs.workflow.xml","smsc.workflow.xml"};
		for (String wf: workflows){
			String workflow = localCfgDir + File.separator + wf;
			String remoteWorkflow = "/user/dbadmin/mtccore/" + wf;
			fs.copyFromLocalFile(new Path(workflow), new Path(remoteWorkflow));
		}
		String localTargetFolder = "C:\\mydoc\\myprojects\\log.analysis\\mtccore\\target\\";
		String localLibFolder = "C:\\mydoc\\myprojects\\log.analysis\\mtccore\\lib\\";
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
	
	public void realCopyXml(String defaultFs, String localCfgDir) throws Exception{
		Configuration conf = new Configuration();
    	conf.set("fs.defaultFS", defaultFs);
    	FileSystem fs = FileSystem.get(conf);
		
    	String[] workflows = new String[]{"testsgs.workflow.xml"};
		for (String wf: workflows){
			String workflow = localCfgDir + File.separator + wf;
			String remoteWorkflow = "/user/dbadmin/mtccore/" + wf;
			fs.copyFromLocalFile(new Path(workflow), new Path(remoteWorkflow));
		}
	}
	
	@Test
	public void testCopyXml() {
		copyXml("hdfs://192.85.247.104:19000", "C:\\mydoc\\myprojects\\log.analysis\\mtccore\\src\\main\\resources");
	}
	
	public void copyXml(final String defaultFs, final String localCfgDir) {
		try {
			if (defaultFs.contains("127.0.0.1")){
				realCopyXml(defaultFs, localCfgDir);
			}else{
				UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			    ugi.doAs(new PrivilegedExceptionAction<Void>() {
			      public Void run() throws Exception {
			    	  realCopyXml(defaultFs, localCfgDir);
					return null;
			      }
			    });
			}
		}catch(Exception e){
			logger.error("", e);
		}
	}
}
