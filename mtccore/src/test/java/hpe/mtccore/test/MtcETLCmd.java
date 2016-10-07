package hpe.mtccore.test;

import java.io.File;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.junit.Test;

import etl.cmd.SftpCmd;
import etl.cmd.test.TestETLCmd;
import etl.util.SchemaUtils;
import etl.util.Util;

public class MtcETLCmd extends TestETLCmd{
	
	public static final Logger logger = Logger.getLogger(MtcETLCmd.class);
	public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	
	@Test
	public void testGenSMSCSchemaFromDB(){
		PropertiesConfiguration pc = Util.getPropertiesConfig("etlengine.properties");
		boolean ret = SchemaUtils.genLogicSchemaFromDB(pc, "SMSC", "smsc.schema");
	}
	
	//@Test
	public void testGenSgsiwfSchemaFromDB(){
		PropertiesConfiguration pc = Util.getPropertiesConfig("etlengine.properties");
		boolean ret = SchemaUtils.genLogicSchemaFromDB(pc, "sgsiwf", "sgsiwf.schema");
	}
	
	@Test
	public void setupLabETLCfg() {
		setupETLCfg(this.getDefaultFS(), this.getProjectFolder());
	}
	
	public void realSetupEtlCfg(String defaultFs, String projectFolder) throws Exception{
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
		String localCfgDir = projectFolder + File.separator + "mtccore" + File.separator + "src" + 
				File.separator + "main" + File.separator + "resources";
		
		//copy etlengine.properties
		String[] propertyFiles = new String[]{"etlengine.properties", "smsc_file_table_mapping.properties"};
		for (String properties: propertyFiles){	
			fs.copyFromLocalFile(false, true, new Path(localCfgDir + File.separator + properties), new Path("/user/" + super.getOozieUser() + "/mtccore/lib/"+properties));
		}
		//copy config
		File localDir = new File(localCfgDir);
		String[] cfgs = localDir.list();
		for (String cfg:cfgs){
			String lcfg = localCfgDir + File.separator + cfg;
			String rcfg = remoteEtlcfg + "/" + cfg;
			fs.copyFromLocalFile(false, true, new Path(lcfg), new Path(rcfg));
		}
		
		//copy schema
		String[] schemas = new String[]{"smsc.schema", "sgsiwf.schema"};
		for (String schema: schemas){
			String workflow = localCfgDir + File.separator + schema;
			String remoteWorkflow = schemadir + "/" + schema;
			fs.copyFromLocalFile(false, true, new Path(workflow), new Path(remoteWorkflow));
		}
		
		//copy workflow
		String[] workflows = new String[]{"sgs.prd.workflow.xml","smsc.prd.workflow.xml", "sgs.lab.workflow.xml","smsc.lab.workflow.xml", 
				"sgs.coordinator.xml", "smsc.coordinator.xml"};
		for (String wf: workflows){
			String workflow = localCfgDir + File.separator + wf;
			String remoteWorkflow = "/user/" + super.getOozieUser() + "/mtccore/" + wf;
			fs.copyFromLocalFile(false, true, new Path(workflow), new Path(remoteWorkflow));
		}
		
		//copy lib
		String mtcLocalTargetFolder = projectFolder + File.separator + "mtccore" + File.separator + "target" + File.separator;
		String[] libNames = new String[]{"mtccore-0.1.0.jar"};
		//String[] libNames = new String[]{"mtccore-0.1.0.jar", "mtccore-0.1.0-jar-with-dependencies.jar", "mtccore-0.1.0-test-jar-with-dependencies.jar"};
		String remoteLibFolder="/user/" + super.getOozieUser() + "/mtccore/lib/";
		for (String libName:libNames){
			fs.copyFromLocalFile(false, true, new Path(mtcLocalTargetFolder + libName), new Path(remoteLibFolder+libName));
		}
		
		String preloadLocalTargetFolder = projectFolder + File.separator + "preload" + File.separator + "target" + File.separator;
		String remoteShareLibFolder="/user/" + super.getOozieUser() + "/share/lib/preload/lib/";
		String libName = "preload-0.1.0.jar";
		fs.delete(new Path(remoteShareLibFolder + libName), true);
		fs.copyFromLocalFile(false, true, new Path(preloadLocalTargetFolder + libName), new Path(remoteShareLibFolder+libName));
	}
	
	public void setupETLCfg(final String defaultFs, final String localCfgDir) {
		try {
			if (defaultFs.contains("127.0.0.1") || defaultFs.contains("localhost")){
				realSetupEtlCfg(defaultFs, localCfgDir);
			}else{
				UserGroupInformation ugi = UserGroupInformation.createProxyUser(super.getOozieUser(), UserGroupInformation.getLoginUser());
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
	
	@Test
	public void getData(){
		String remoteEtlcfg = "/mtccore/etlcfg/";
		String sftpCmdProperties="sgsiwf.sftp.properties";
		SftpCmd cmd = new SftpCmd("wfName", "wfid", remoteEtlcfg+sftpCmdProperties, this.getDefaultFS(), null);
		List<String> ret = cmd.sgProcess();
		logger.info(ret);
	}
	
	@Test
	public void testCopyXml() {
		copyXml(this.getDefaultFS(), this.getProjectFolder());
	}
	
	public void realCopyXml(String defaultFs, String projectFolder) throws Exception{
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
		
		//copy etlengine.properties
		String etlengineProp= "etlengine.properties";
		String localCfgDir = projectFolder + File.separator + "mtccore" + File.separator + "src" + 
				File.separator + "main" + File.separator + "resources";
		fs.copyFromLocalFile(new Path(localCfgDir + File.separator + etlengineProp), new Path("/user/" + super.getOozieUser() + "/mtccore/lib/"+etlengineProp));
		
		//copy config
		File localDir = new File(localCfgDir);
		String[] cfgs = localDir.list();
		for (String cfg:cfgs){
			String lcfg = localCfgDir + File.separator + cfg;
			String rcfg = remoteEtlcfg + "/" + cfg;
			fs.copyFromLocalFile(new Path(lcfg), new Path(rcfg));
		}
		
		//copy schema
		String[] schemas = new String[]{"smsc.schema", "sgsiwf.schema"};
		for (String schema: schemas){
			String workflow = localCfgDir + File.separator + schema;
			String remoteWorkflow = schemadir + "/" + schema;
			fs.copyFromLocalFile(new Path(workflow), new Path(remoteWorkflow));
		}
		
		//copy workflow
		String[] workflows = new String[]{"sgs.workflow.xml","smsc.workflow.xml"};
		for (String wf: workflows){
			String workflow = localCfgDir + File.separator + wf;
			String remoteWorkflow = "/user/" + super.getOozieUser() + "/mtccore/" + wf;
			fs.copyFromLocalFile(new Path(workflow), new Path(remoteWorkflow));
		}
	}
	
	public void copyXml(final String defaultFs, final String localCfgDir) {
		try {
			if (defaultFs.contains("127.0.0.1")){
				realCopyXml(defaultFs, localCfgDir);
			}else{
				UserGroupInformation ugi = UserGroupInformation.createProxyUser(super.getOozieUser(), UserGroupInformation.getLoginUser());
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
	
	@Test
	public void cleanUp() {
		cleanUp(getDefaultFS());
	}
	
	public void realCleanUp(String defaultFs) throws Exception{
		Configuration conf = new Configuration();
    	conf.set("fs.defaultFS", defaultFs);
    	FileSystem fs = FileSystem.get(conf);
		String xmldir = "/mtccore/xmldata";
		String csvdir = "/mtccore/csvdata";
		String schemadir = "/mtccore/schema";
		String schemaHistoryDir = "/mtccore/schemahistory";
		fs.delete(new Path(csvdir), true);
		fs.delete(new Path(xmldir), true);
		fs.delete(new Path(schemadir), true);
		fs.delete(new Path(schemaHistoryDir), true);
		
		fs.mkdirs(new Path(csvdir));
		fs.mkdirs(new Path(xmldir));
		fs.mkdirs(new Path(schemadir));
		fs.mkdirs(new Path(schemaHistoryDir));
	}
	
	public void cleanUp(final String defaultFs) {
		try {
			if (defaultFs.contains("127.0.0.1")){
				realCleanUp(defaultFs);
			}else{
				UserGroupInformation ugi = UserGroupInformation.createProxyUser(super.getOozieUser(), UserGroupInformation.getLoginUser());
			    ugi.doAs(new PrivilegedExceptionAction<Void>() {
			      public Void run() throws Exception {
			    	  realCleanUp(defaultFs);
					return null;
			      }
			    });
			}
		}catch(Exception e){
			logger.error("", e);
		}
	}

	@Override
	public String getResourceSubFolder() {
		return null;
	}
}
