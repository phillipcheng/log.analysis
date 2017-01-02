package etl.cmd;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import etl.engine.ETLCmd;
import etl.engine.ProcessMode;
import etl.log.ETLLog;
import etl.util.ConfigKey;
import etl.util.ScriptEngineUtil;
import etl.util.VarType;

public class BackupCmd extends ETLCmd{
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(BackupCmd.class);

	//cfgkey
	public static final @ConfigKey String cfgkey_data_history_folder="data-history-folder";
	public static final @ConfigKey(type=String[].class) String cfgkey_Folder_filter="file.folder";
	public static final @ConfigKey(type=String[].class) String cfgkey_file_filter="file.filter";

	private String dataHistoryFolder;
	private String[] fileFolders;
	private String[] fileFilters;
	private String destZipFile;
	
	public BackupCmd(){
		super();
	}
	
	public BackupCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	public BackupCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs, ProcessMode pm){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, pm);
	}
	
	public BackupCmd(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs, ProcessMode pm){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
		this.dataHistoryFolder = super.getCfgString(cfgkey_data_history_folder, null);
		String[] ffExps = super.getCfgStringArray(cfgkey_Folder_filter);
		fileFolders = new String[ffExps.length];
		for (int i=0; i<ffExps.length; i++){
			String ffExp = ffExps[i];
			fileFolders[i] = (String) ScriptEngineUtil.eval(ffExp, VarType.STRING, super.getSystemVariables());
		}
		this.fileFilters = super.getCfgStringArray(cfgkey_file_filter);
		this.destZipFile=this.dataHistoryFolder+wfid+".zip";
	}

	@Override
	public List<String> sgProcess(){
		List<String> logInfo = new ArrayList<String>();
		int totalFiles = 0;
		ZipOutputStream zos = null;
		try {
			Path destpath=new Path(destZipFile);
			FSDataOutputStream fos = fs.create(destpath);
			zos = new ZipOutputStream(fos);	
			for (int i = 0; i < fileFolders.length; i++) {
				int n = zipFolder(zos, fileFolders[i],fileFilters[i]);
				logger.info(String.format("%d files found for fold %s", n, fileFolders[i]));
				totalFiles +=n;
			}
		}catch (Exception e) {
			logger.error(new ETLLog(this, null, e), e);
		}finally{
			try {
				if (zos!=null){
					zos.close();
				}
			} catch(IOException e) {
				logger.error("Exception closing IO streams ...! ", e);
			}
		}
		logInfo.add(totalFiles+"");
		return logInfo;
	}

	/**
	 * 
	 * @param dirpath
	 * @param fileFilter
	 * @return number of files zipped
	 */
	public int zipFolder(ZipOutputStream zos, String dirpath ,String fileFilter) {
		try {	
			List<String> fileNames = new ArrayList<String>();
			String exp=fileFilter;
			Object output =ScriptEngineUtil.eval(exp, VarType.OBJECT, super.getSystemVariables());
			if(output instanceof ArrayList){  
				ArrayList<String> out=(ArrayList<String>)output;
				fileNames.addAll(out); 
			}else if (output instanceof String[]) {
				String[] out=(String[])output;
				fileNames.addAll(Arrays.asList(out));
			}else if(output instanceof String){
				String regexp=(String)output;
				fileNames=filterFiles(regexp, dirpath);
			}else{
				logger.error(String.format("type %s not supported for %s", output, exp));
			}
			zipFiles(zos, dirpath, fileNames);
			return fileNames.size();
		} catch (Exception e) {
			logger.error(" ", e);
		}
		return 0;
	}
	
    //filters and gets file list
	public List<String> filterFiles(final String exp,String dirpath) throws FileNotFoundException, IOException {   
		List<String> fileNameList = new ArrayList<String>();
		PathFilter PATH_FILTER = new PathFilter() { 
			public boolean accept(Path path) { 
				Pattern pattern = Pattern.compile(exp);
				Matcher m = pattern.matcher(path.getName());
				return m.matches();
			}      
		}; 
		Path inputPath = new Path(dirpath);
		FileStatus[] status = fs.listStatus(inputPath, PATH_FILTER);
		for (int i = 0; i < status.length; i++) {
			if (!status[i].isDirectory()){
				fileNameList.add(status[i].getPath().getName());
			}
		}
		return fileNameList;
	}
	
	//Zips the files followed by remove
	public void zipFiles(ZipOutputStream zos, String dirpath, List<String> fileNames){
		try {
			List<String> directoryFiles=new ArrayList<String>();
			for (String fileName:fileNames){
				// check for subfiles
				String dirLocation=dirpath+ File.separator + fileName;
				Path pathDir = new Path(dirLocation);
				FileStatus fstatus = fs.getFileStatus(pathDir);
				if(fstatus.isDirectory()){
					FileStatus[] listStatus = fs.listStatus(pathDir);
					for (FileStatus stat: listStatus) {
						directoryFiles.add(stat.getPath().getName());
					}
					zipFiles(zos, dirLocation,directoryFiles);
					fs.delete(pathDir,false);
					continue;
				}   
				// add the file to zip
				logger.info("Adding file "+fileName); 
				ZipEntry ze= new ZipEntry(fileName);
				zos.putNextEntry(ze);
				Path srcpath=new Path(dirpath+ File.separator + fileName);
				logger.info("src path is "+srcpath);
				FSDataInputStream in = fs.open(srcpath);
				int buffersize =8192;
				byte[] buffer = new byte[buffersize];
				int count;
				while((count = in.read(buffer)) != -1){
					zos.write(buffer,0,count);    
				}
				fs.delete(srcpath,false);
				zos.closeEntry();
				in.close();
			}
		}catch (Exception e) {
			logger.error(" ", e);
		}
	}

}
