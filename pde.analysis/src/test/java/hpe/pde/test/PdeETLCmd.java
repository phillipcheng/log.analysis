package hpe.pde.test;

import java.io.File;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.junit.Test;

import etl.cmd.test.TestETLCmd;

public class PdeETLCmd extends TestETLCmd{
	public static final Logger logger = Logger.getLogger(PdeETLCmd.class);
	String pdeCsvProp = "preload.pde.csv.properties";
	String pdeFixProp = "preload.pde.fix.properties";
	
	@Override
	public String getResourceSubFolder() {
		return null;
	}
	
	@Test
	public void setupLabETLCfg() {
		setupETLCfg(super.getDefaultFS(), super.getLocalFolder());
	}
	
	public void realSetupEtlCfg(String defaultFs, String localCfgDir) throws Exception{
		Configuration conf = new Configuration();
    	conf.set("fs.defaultFS", defaultFs);
    	FileSystem fs = FileSystem.get(conf);
    	
    	String remoteLibFolder = "/user/"+super.getOozieUser()+"/pde";
    	Path remoteLibPath = new Path(remoteLibFolder);
    	if (fs.exists(remoteLibPath)){
    		fs.delete(remoteLibPath, true);
    	}
    	//
    	String workflow = localCfgDir + File.separator + "workflow.xml";
		String remoteWorkflow = remoteLibFolder + File.separator + "workflow.xml";
		fs.delete(new Path(remoteWorkflow), true);
		fs.copyFromLocalFile(new Path(workflow), new Path(remoteWorkflow));
		
		//copy libs
		String localTargetFolder = super.getProjectFolder()+File.separator+ "pde.analysis" +File.separator+"target"+File.separator;
		String libName = "pde-0.1.0-jar-with-dependencies.jar";
		fs.copyFromLocalFile(new Path(localTargetFolder + libName), new Path(remoteLibFolder + "/lib/" +libName));
		
		String localLibFolder = super.getProjectFolder()+File.separator+ "pde.analysis" +File.separator+"lib"+File.separator;
		libName = "vertica-jdbc-7.0.1-0.jar";
		fs.copyFromLocalFile(new Path(localLibFolder + libName), new Path(remoteLibFolder+ "/lib/" + libName));
		
		String preloadLocalTargetFolder = super.getProjectFolder() + File.separator + "preload" + File.separator + "target" + File.separator;
		String remoteShareLibFolder="/user/" + super.getOozieUser() + "/share/lib/preload/lib/";
		libName = "preload-0.1.0.jar";
		fs.delete(new Path(remoteShareLibFolder + libName), true);
		fs.copyFromLocalFile(new Path(preloadLocalTargetFolder + libName), new Path(remoteShareLibFolder+libName));
		
		//copy etlcfg
		String remoteCfgFolder = "/pde/etlcfg/";
		Path remoteCfgPath = new Path(remoteCfgFolder);
		if (fs.exists(remoteCfgPath)){
			fs.delete(new Path(remoteCfgFolder), true);
		}
		File localDir = new File(localCfgDir);
		String[] cfgs = localDir.list();
		for (String cfg:cfgs){
			String lcfg = localCfgDir + File.separator + cfg;
			String rcfg = remoteCfgFolder + "/" + cfg;
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
	
	@Test
	public void testCopyXml() {
		copyXml(super.getDefaultFS(), super.getLocalFolder());
	}
	
	public void realCopyXml(String defaultFs, String localCfgDir) throws Exception{
		Configuration conf = new Configuration();
    	conf.set("fs.defaultFS", defaultFs);
    	FileSystem fs = FileSystem.get(conf);
		
    	String remoteCfgFolder = "/pde/etlcfg/";
    	File localDir = new File(localCfgDir);
		String[] cfgs = localDir.list();
		for (String cfg:cfgs){
			String lcfg = localCfgDir + File.separator + cfg;
			String rcfg = remoteCfgFolder + "/" + cfg;
			fs.copyFromLocalFile(new Path(lcfg), new Path(rcfg));
		}
		
    	String[] workflows = new String[]{"workflow.xml"};
		for (String wf: workflows){
			String workflow = localCfgDir + File.separator + wf;
			String remoteWorkflow = "/user/dbadmin/pde/" + wf;
			fs.copyFromLocalFile(new Path(workflow), new Path(remoteWorkflow));
		}
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
