package etl.cmd.test;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import etl.cmd.ShellCmd;
import etl.engine.InvokeMapper;
import etl.util.OSType;
import etl.util.PropertiesUtil;
import etl.util.SystemUtil;
import etl.util.Util;

public class TestShellCmd extends TestETLCmd {
	public static final Logger logger = LogManager.getLogger(ShellCmd.class);
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

	public String getResourceSubFolder(){
		return "shell/";
	}
	
	@Test
	public void test1() throws Exception {
		try{
			String remoteCfgFolder = "/pde/cfg/";
			String remoteOutputFolder="/pde/output/";
			OSType os = SystemUtil.getOsType();
			//setup testing env
			String inputFile = "input.txt";
			String shellProp = "shellCmd1Win.properties";
			String executableFile = "copyFile.bat";
			String executableDir = "C:\\Users";
			if (os == OSType.MAC || os == OSType.UNIX){
				shellProp = "shellCmd1Unix.properties";
				executableFile = "copyfile.sh";
				executableDir = "/tmp";
			}
			
			getFs().delete(new Path(remoteOutputFolder), true);
			getFs().copyFromLocalFile(new Path(getLocalFolder()+shellProp), new Path(remoteCfgFolder+shellProp));
			getFs().copyFromLocalFile(new Path(getLocalFolder()+inputFile), new Path(remoteCfgFolder+inputFile));
			PropertiesConfiguration pc = PropertiesUtil.getMergedPCFromDfs(getFs(), remoteCfgFolder + shellProp);
			File sourceDir = new File(pc.getString("srcfolder"));
			File destDir = new File(pc.getString("destfolder"));
			FileUtils.cleanDirectory(destDir); 
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
			FileInputFormat.addInputPath(job, new Path(remoteCfgFolder+inputFile));
			FileOutputFormat.setOutputPath(job, new Path(remoteOutputFolder));
			job.waitForCompletion(true);

			//check results

			File[] srcFiles= sourceDir.listFiles();
			File[] destFiles= destDir.listFiles();
			assertTrue(srcFiles.length==destFiles.length);

			boolean result=true;
			for (int i = 0; i < srcFiles.length; i++) {
				if(!srcFiles[i].getName().equals(destFiles[i].getName())) {
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
	public void test2() throws Exception {
		try{
			String remoteCfgFolder = "/pde/cfg/";
			String remoteOutputFolder="/pde/output/";
			OSType os = SystemUtil.getOsType();
			//setup testing env
			String inputFile = "input2.txt";
			String shellProp = "shellCmd2Win.properties";
			String executableFile = "copyFile.bat";
			String executableDir = "C:\\Users";
			if (os == OSType.MAC || os == OSType.UNIX){
				shellProp = "shellCmd1Unix.properties";
				executableFile = "copyfile.sh";
				executableDir = "/tmp";
			}
			PropertiesConfiguration pc = PropertiesUtil.getMergedPCFromDfs(getFs(), remoteCfgFolder + shellProp);
			File sourceDir = new File(pc.getString("srcfolder"));
			File destDir = new File(pc.getString("destfolder"));
			FileUtils.cleanDirectory(destDir); 
			
			getFs().delete(new Path(remoteOutputFolder), true);
			getFs().copyFromLocalFile(new Path(getLocalFolder()+shellProp), new Path(remoteCfgFolder+shellProp));
			getFs().copyFromLocalFile(new Path(getLocalFolder()+inputFile), new Path(remoteCfgFolder+inputFile));
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
			FileInputFormat.addInputPath(job, new Path(remoteCfgFolder+inputFile));
			FileOutputFormat.setOutputPath(job, new Path(remoteOutputFolder));
			job.waitForCompletion(true);

			//check results
			assertTrue(sourceDir.exists());

			assertTrue(destDir.exists());

			File[] srcFiles= sourceDir.listFiles();
			File[] destFiles= sourceDir.listFiles();
			assertTrue(srcFiles.length==destFiles.length);

			boolean result=true;
			for (int i = 0; i < srcFiles.length; i++) {
				if(!srcFiles[i].getName().equals(destFiles[i].getName())) {
					result=false;
				}
			}
			assertTrue(result);
			logger.info("Results verified successfully ..!!");
		} catch (Exception e) {
			logger.error("", e);
		}
	}
}




