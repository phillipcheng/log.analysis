package etl.cmd.test;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.junit.Test;

import etl.cmd.ShellCmd;
import etl.engine.InvokeMapper;
import etl.util.Util;

public class TestShellCmd extends TestETLCmd {
	public static final Logger logger = Logger.getLogger(ShellCmd.class);
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

	@Test
	public void testShellCmd() throws Exception {
					try{
					String remoteCfgFolder = "/pde/cfg";
					String remoteOutputFolder="/pde/output";
					//setup testing env
					String shellProp = "shellCmd1.properties";
					String executableFile = "copyFile.bat";
					String executableDir = "C:\\Users";
					getFs().delete(new Path(remoteOutputFolder), true);
					getFs().copyFromLocalFile(new Path(getLocalFolder()+shellProp), new Path(remoteCfgFolder+shellProp));
					//run job  
					FileUtils.copyFileToDirectory(new File(getLocalFolder()+executableFile), new File(executableDir));
					getConf().set(InvokeMapper.cfgkey_cmdclassname, "etl.cmd.ShellCmd");
					getConf().set(InvokeMapper.cfgkey_wfid, sdf.format(new Date()));
					getConf().set(InvokeMapper.cfgkey_staticconfigfile, remoteCfgFolder+shellProp);
					Job job = Job.getInstance(getConf(), "testShellCmd");
					job.setMapperClass(etl.engine.InvokeMapper.class);
					job.setNumReduceTasks(0);//no reducer
					job.setOutputKeyClass(Text.class);
					job.setOutputValueClass(NullWritable.class);
					FileInputFormat.setInputDirRecursive(job, true);
					FileInputFormat.addInputPath(job, new Path(remoteCfgFolder+shellProp));
					FileOutputFormat.setOutputPath(job, new Path(remoteOutputFolder));
					job.waitForCompletion(true);

					//check results
					PropertiesConfiguration pc = Util.getPropertiesConfigFromDfs(getFs(), remoteCfgFolder + shellProp);
					File sourceDir = new File(pc.getString("srcfolder"));
					assertTrue(sourceDir.exists());

					File destDir = new File(pc.getString("destfolder"));
					assertTrue(destDir.exists());

					File[] srcFiles= sourceDir.listFiles();
					File[] destFiles= sourceDir.listFiles();
					assert(srcFiles.length==destFiles.length);

					boolean result=true;
					for (int i = 0; i < srcFiles.length; i++) {
						if(!srcFiles[i].getName().equals(destFiles[i].getName()))
						{
							result=false;
						}
					}
					assertTrue(result);
					logger.info("Results verified successfully ..!!");
				} catch (Exception e) {
					logger.error("", e);
				}
	}
	
	
	@Test
	public void remoteTest1() throws Exception{
		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				testShellCmd();
				return null;
			}
		});

	}
}




