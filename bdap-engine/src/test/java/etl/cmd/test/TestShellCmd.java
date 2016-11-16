package etl.cmd.test;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import bdap.util.OSType;
import bdap.util.SystemUtil;
import etl.cmd.ShellCmd;
import etl.engine.EngineUtil;

public class TestShellCmd extends TestETLCmd {
	public static final Logger logger = LogManager.getLogger(ShellCmd.class);
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

	public String getResourceSubFolder(){
		return "shell/";
	}
	
	private static final String cmdClassName="etl.cmd.ShellCmd";
	
	@Test
	public void test1() throws Exception {
		String remoteInputFolder = "/pde/input/";
		String remoteOutputFolder="/pde/output/";
		OSType os = SystemUtil.getOsType();
		//setup testing env
		String[] inputFiles = new String[]{"input.txt"};
		String shellProp = "shellCmd1Win.properties";
		String executableFile = "copyFile.bat";
		String executableDir = "C:\\Users";
		if (os == OSType.MAC || os == OSType.UNIX){
			shellProp = "shellCmd1Unix.properties";
			executableFile = "copyfile.sh";
			executableDir = "/tmp";
		}
		
		PropertiesConfiguration pc = EngineUtil.getInstance().getMergedPC(this.getResourceSubFolder() + shellProp);
		File sourceDir = new File(pc.getString("srcfolder"));
		File destDir = new File(pc.getString("destfolder"));
		FileUtils.deleteDirectory(destDir); 
		FileUtils.forceMkdir(destDir);
		//run job  
		FileUtils.copyFileToDirectory(new File(getLocalFolder()+executableFile), new File(executableDir));
		List<String> ret = super.mapTest(remoteInputFolder, remoteOutputFolder, shellProp, inputFiles, cmdClassName, false);
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
	}
	
	@Test
	public void test2() throws Exception {
		String remoteInputFolder = "/pde/input/";
		String remoteOutputFolder="/pde/output/";
		OSType os = SystemUtil.getOsType();
		//setup testing env
		String[] inputFiles = new String[]{"input2.txt"};
		String shellProp = "shellCmd2Win.properties";
		String executableFile = "copyFile.bat";
		String executableDir = "C:\\Users";
		if (os == OSType.MAC || os == OSType.UNIX){
			shellProp = "shellCmd1Unix.properties";
			executableFile = "copyfile.sh";
			executableDir = "/tmp";
		}
		
		PropertiesConfiguration pc = EngineUtil.getInstance().getMergedPC(this.getResourceSubFolder() + shellProp);
		File sourceDir = new File(pc.getString("srcfolder"));
		File destDir = new File(pc.getString("destfolder"));
		FileUtils.deleteDirectory(destDir); 
		FileUtils.forceMkdir(destDir);
		
		//run job  
		FileUtils.copyFileToDirectory(new File(getLocalFolder()+executableFile), new File(executableDir));
		List<String> ret = super.mapTest(remoteInputFolder, remoteOutputFolder, shellProp, inputFiles, cmdClassName, false);

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
	}
	
	@Test
	public void shellWithResult() throws Exception {
		String remoteInputFolder = "/shellcmd/input/";
		String remoteOutputFolder="/shellcmd/output/";
		OSType os = SystemUtil.getOsType();
		//setup testing env
		String[] inputFiles = new String[]{"input.txt"};
		String shellProp = "shellCmd1Win.properties";
		String executableFile = "copyFile.bat";
		String executableDir = "C:\\Users";
		if (os == OSType.MAC || os == OSType.UNIX){
			shellProp = "shellCmd1Unix.properties";
			executableFile = "copyfile.sh";
			executableDir = "/tmp";
		}
		
		PropertiesConfiguration pc = EngineUtil.getInstance().getMergedPC(this.getResourceSubFolder() + shellProp);
		File sourceDir = new File(pc.getString("srcfolder"));
		File destDir = new File(pc.getString("destfolder"));
		FileUtils.deleteDirectory(destDir); 
		FileUtils.forceMkdir(destDir);
		//run job  
		FileUtils.copyFileToDirectory(new File(getLocalFolder()+executableFile), new File(executableDir));
		List<String> ret = super.mapTest(remoteInputFolder, remoteOutputFolder, shellProp, inputFiles, cmdClassName, false);
		//check results
		logger.info(String.format("ret:\n%s", String.join("\n", ret)));
		assertTrue(ret.size()==4);
		assertTrue(ret.get(0).contains("key"));
	}
}
