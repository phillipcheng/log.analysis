package etl.cmd;



import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import etl.cmd.dynschema.DynSchemaCmd;
import etl.engine.ETLCmd;
import etl.util.Util;

public class BackupCmd extends ETLCmd{
	public static final Logger logger = Logger.getLogger(BackupCmd.class);

	public static final String cfgkey_data_history_folder="data-history-folder";
	public static final String cfgkey_destination_zip_folder="destination-zip-folder";
	public static final String cfgkey_Folder_filter="folder.filter";
	public static final String cfgkey_file_filter="file.filter";
	public static final String dynCfg_Key_XML_FILES="raw.xml.files";
	public static final String dynCfg_Key_WFID_FILTER="WFID";
	public static final String dynCfg_Key_ALL_FILTER="ALL";


	private String dataHistoryFolder;
	private String destinationZipFolder;
	private FileSystem fileSys;
	private String[] folderFilter;
	private String[] fileFilter;
	private ZipOutputStream zos;

	public BackupCmd(String wfid, String staticCfg, String inDynCfg, String outDynCfg, String defaultFs){
		super(wfid, staticCfg, inDynCfg, outDynCfg, defaultFs);
		this.dataHistoryFolder = pc.getString(cfgkey_data_history_folder);
		this.folderFilter = pc.getStringArray(cfgkey_Folder_filter);
		this.fileFilter = pc.getStringArray(cfgkey_file_filter);
		this.destinationZipFolder=this.dataHistoryFolder+wfid+".zip";
	}

	@Override
	public List<String> process(String param, Mapper<Object, Text, Text, NullWritable>.Context context) {

		logger.info("param: "+param);

		Map<String, String> pm = Util.parseMapParams(param);
		if (pm.containsKey(cfgkey_data_history_folder)){
			this.dataHistoryFolder = pm.get(cfgkey_data_history_folder);
		}
		if (pm.containsKey(cfgkey_destination_zip_folder)){
			this.destinationZipFolder = pm.get(cfgkey_destination_zip_folder);
		}
		if (pm.containsKey(cfgkey_Folder_filter)){

			this.folderFilter=pm.get(cfgkey_Folder_filter).split(" ");
		}
		if (pm.containsKey(cfgkey_file_filter)){
			this.fileFilter = pm.get(cfgkey_file_filter).split(" ");
		}

		try {
			fileSys=FileSystem.get(this.getHadoopConf());
			if((!(destinationZipFolder.startsWith(dataHistoryFolder)&&destinationZipFolder.endsWith(".zip"))))
			{
				logger.info("The Destination Zip folder provided is not valid for wfid "+wfid);
				return null;
			}
			Path destpath=new Path(destinationZipFolder);
			FSDataOutputStream fos = fileSys.create(destpath);
			zos = new ZipOutputStream(fos);	
            int folderFilterLen=folderFilter.length;
            int fileFilterLen=fileFilter.length;
			for (int i = 0; i < folderFilterLen ; i++) 
			{
				if(folderFilterLen>fileFilterLen)
				{
					zipFolder(folderFilter[i], fileFilter[fileFilterLen-1]);
				}
				else 
				{
					zipFolder(folderFilter[i],fileFilter[i]);
				}
			}

			logger.info("Finished Job :  "+wfid);
		} 
		catch (NullPointerException e) {
			logger.error("Null Pointer exception ...!  ", e);
		}
		catch (IOException e) {
			logger.error("IO Exception...!  ", e);
		}
		catch (Exception e) {
			logger.error(" ", e);
		}
		finally 
		{
			try 
			{  // closed safely 
				zos.close();
			} 
			catch(IOException e) {
				logger.error("Exception closing IO streams ...! ", e);
			}
		}

		return null;
	}


	//Zips files based on filters for the arbitary folders 
	public  void zipFolder(String dirpath ,String fileFilter)
	{
		try{
			Path inputPath = new Path(dirpath);
			fileSys = inputPath.getFileSystem(this.getHadoopConf());
			FileStatus[] status = fileSys.listStatus(inputPath);
			for (int i=0;i<status.length;i++){
				Path path =status[i].getPath();
				if(status[i].isFile()){
					String fileName=path.getName();
					if(fileFilter.equals(dynCfg_Key_XML_FILES))
					{
						zipFile(dirpath,fileName);
					}
					if((fileFilter.equals(dynCfg_Key_WFID_FILTER)))
					{
						if(fileName.startsWith(wfid, 0))
						{
							zipFile(dirpath,fileName);
						}
					}
					else if(fileFilter.equals(dynCfg_Key_ALL_FILTER))
					{
						zipFile(dirpath,fileName);	
					}

				}
			}
		}
		catch(FileNotFoundException e)
		{
			logger.error("Folder does not exists .! Exception occured...! ", e);
		}
		catch (NullPointerException e) {
			logger.error("Null Pointer exception ...!  ", e);
		}
		catch (IOException e) {
			logger.error("File IO exception occured...! ", e);
		}
		catch (Exception e) {
			logger.error(" ", e);
		}
	}

	//Zips the files followed by remove
	public void zipFile(String dirpath,String fileName)
	{

		try {
			logger.info("Adding file "+fileName); 
			ZipEntry ze= new ZipEntry(fileName);
			zos.putNextEntry(ze);
			Path srcpath=new Path(dirpath+ File.separator + fileName);
			logger.info("src path is "+srcpath);
			FSDataInputStream in = fileSys.open(srcpath);
			int buffersize =8192;
			byte[] buffer = new byte[buffersize];
			int count;
			while((count = in.read(buffer)) != -1)
			{
				zos.write(buffer,0,count);    
			}
			fileSys.delete(srcpath,false);   //Delete the file from source
			zos.closeEntry();
			in.close();

		} 	catch(FileNotFoundException e)
		{
			logger.error("Folder does not exists .! Exception occured...! ", e);
		}
		catch (NullPointerException e) {
			logger.error("Null Pointer exception ...!  ", e);
		}
		catch (IOException e) {
			// TODO: handle exception
			logger.error("File IO exception occured...! ", e);
		}
		catch (Exception e) {
			// TODO: handle exception
			logger.error(" ", e);
		}

	}

}
