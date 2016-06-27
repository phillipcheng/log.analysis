package etl.cmd;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import etl.engine.ETLCmd;
import etl.util.DBUtil;

public class BackupCmd extends ETLCmd{
	public static final Logger logger = Logger.getLogger(BackupCmd.class);

	public static final String cfgkey_data_history_folder="data-history-folder";
	public static final String cfgkey_Folder_filter="folder.filter";
	public static final String cfgkey_file_filter="file.filter";
	public static final String dynCfg_Key_WFID_FILTER="WFID";
	public static final String dynCfg_Key_ALL_FILTER="ALL";

	private String dataHistoryFolder;
	private String[] fileFolders;
	private String[] fileFilters;
	private String destZipFile;
	private ZipOutputStream zos;

	public BackupCmd(String wfid, String staticCfg, String inDynCfg, String outDynCfg, String defaultFs){
		super(wfid, staticCfg, inDynCfg, outDynCfg, defaultFs);
		this.dataHistoryFolder = pc.getString(cfgkey_data_history_folder);
		this.fileFolders = pc.getStringArray(cfgkey_Folder_filter);
		this.fileFilters = pc.getStringArray(cfgkey_file_filter);
		this.destZipFile=this.dataHistoryFolder+wfid+".zip";
	}

	@Override
	public List<String> process(long offset, String row, Mapper<LongWritable, Text, Text, NullWritable>.Context context){
		try {
			Path destpath=new Path(destZipFile);
			FSDataOutputStream fos = fs.create(destpath);
			zos = new ZipOutputStream(fos);	
			for (int i = 0; i < fileFolders.length; i++) {
				zipFolder(fileFolders[i],fileFilters[i]);
			}
		}catch (Exception e) {
			logger.error(" ", e);
		}finally{
			try {
				zos.close();
			} catch(IOException e) {
				logger.error("Exception closing IO streams ...! ", e);
			}
		}
		return null;
	}


	//Zips files based on filters for the arbitary folders 
	public void zipFolder(String dirpath ,String fileFilter) {
		try {
			List<String> fileNames = new ArrayList<String>();
			if (dynCfg_Key_WFID_FILTER.equals(fileFilter) || dynCfg_Key_ALL_FILTER.equals(fileFilter)){
				Path inputPath = new Path(dirpath);
				FileStatus[] status = fs.listStatus(inputPath);
				for (int i=0;i<status.length;i++){
					Path path =status[i].getPath();
					if(status[i].isFile()){
						String fileName=path.getName();
						if((fileFilter.equals(dynCfg_Key_WFID_FILTER))){
							if(fileName.startsWith(wfid)){
								fileNames.add(fileName);
							}
						}else{
							fileNames.add(fileName);
						}
					}
				}
			}else{
				fileNames = dynCfgMap.get(fileFilter);
			}
			zipFiles(dirpath, fileNames);
		} catch (Exception e) {
			logger.error(" ", e);
		}
	}

	//Zips the files followed by remove
	public void zipFiles(String dirpath, List<String> fileNames){
		try {
			for (String fileName:fileNames){
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
